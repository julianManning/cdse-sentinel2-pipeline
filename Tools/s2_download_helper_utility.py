"""
Sentinel-2 Download Pipeline Workflow

A robust, automated pipeline for querying, filtering, and downloading Sentinel-2
satellite imagery from the Copernicus Data Space Ecosystem (CDSE).

Key Features:
- OData API querying with spatial, temporal, and cloud-cover filtering
  (paginated, so long time ranges are not truncated at 1000 products; end date inclusive).
- Automated Keycloak authentication with age-based token refresh.
- Dynamic spatial overlap reduction using local UTM coordinate reprojection.
- Selection preview (product table, total volume, Esri footprint map) before downloading.
- Sequential or multithreaded downloads with .part files, size checks and live resume logs.

Author: Julian Manning
Created: April 2026 (updated October 2026)
Email: julian.manning@outlook.com
LinkedIn: https://www.linkedin.com/in/julian-manning/
"""

# ---------------- Core / Standard Library ----------------
import os
import sys
import time
import re
import html
import zipfile
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from contextlib import contextmanager

# ---------------- Data Handling ----------------
import pandas as pd

# ---------------- Geospatial ----------------
import geopandas as gpd
from shapely.geometry import mapping, shape as shp_shape, box
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform, unary_union
from shapely.prepared import prep
import shapely.wkt as wkt
from pyproj import Transformer

# ---------------- Visualisation / Mapping ----------------
import folium

# ---------------- Networking / IO ----------------
import requests

# ---------------- Utilities ----------------
from tqdm import tqdm

# ---------------- Optional Dependencies ----------------
try:
    import certifi as _certifi
except Exception:
    _certifi = None

# ---------------- Constants ----------------
CDSE_ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

# CDSE access tokens live ~10 minutes; refresh before that.
TOKEN_MAX_AGE_S = 8 * 60

# ====================================================================================================

def batch_extract_zips(
    source_folder: str,
    destination_folder: str,
    delete_after_extraction: bool = False
) -> None:
    
    """
    Automate the batch extraction of compressed ZIP archives to a target directory.

    This utility iterates through a source folder to identify all ZIP files and 
    unpacks their contents into a specified destination. It includes built-in 
    error handling for corrupted or invalid archives and offers an optional 
    cleanup parameter to delete the original ZIP files post-extraction. This is 
    typically used as a pre-processing step in remote sensing workflows to 
    prepare raw satellite imagery downloads for atmospheric correction or 
    analysis.

    Args:
        source_folder (str): The directory containing the compressed ZIP files.
        destination_folder (str): The target directory where the extracted 
            contents will be stored.
        delete_after_extraction (bool, optional): If True, the source ZIP file 
            will be permanently removed from the disk upon successful 
            extraction. Defaults to False.

    Returns:
        None: Performs file system operations and prints status updates to the 
            console regarding extraction and deletion progress.
    """
    
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    for filename in os.listdir(source_folder):
        if not filename.lower().endswith('.zip'):
            continue

        file_path = os.path.join(source_folder, filename)

        try:
            with zipfile.ZipFile(file_path, 'r') as archive:
                archive.extractall(destination_folder)
            print(f'Extracted: {filename}')

            if delete_after_extraction:
                os.remove(file_path)
                print(f'Deleted: {filename}')

        except zipfile.BadZipFile:
            print(f'Invalid zip file skipped: {filename}')

# ====================================================================================================

def visualise_roi_on_map(
    roi_gdf: gpd.GeoDataFrame,
    epsg: int = 4326,
    use_dissolve: bool = False,
    zoom_start: int = 8,
    tiles: str = "Esri.WorldImagery",
    rectangle_color: str = "#FF6A00",
    rectangle_weight: int = 2,
    polygon_color: str = "#1f78b4",
    polygon_weight: int = 2,
    polygon_fill_opacity: float = 0.1,
    fit_padding: tuple = (40, 40),
    add_polygon: bool = True,
    save_path: Optional[str] = None,
) -> folium.Map:
    
    """
    Generate an interactive map visualization of a Region of Interest (ROI) for spatial verification.

    This function provides a "visual sanity check" by overlaying a GeoDataFrame onto 
    an interactive Folium basemap. It automatically handles coordinate reprojection 
    to a geographic CRS, cleans invalid geometries, and centers the map view 
    on the ROI. The visualization includes both the actual polygon geometry and 
    a bounding box rectangle, allowing users to verify spatial extent, coverage, 
    and alignment against real-world features before initiating large-scale 
    satellite data processing.

    Args:
        roi_gdf (gpd.GeoDataFrame): The input spatial data containing the ROI.
        epsg (int, optional): The geographic CRS for mapping. Defaults to 4326.
        use_dissolve (bool, optional): If True, merges all geometries in the 
            GeoDataFrame into a single unified shape. Defaults to False.
        zoom_start (int, optional): Initial zoom level for the map. Defaults to 8.
        tiles (str, optional): The basemap provider (e.g., "Esri.WorldImagery", "OpenStreetMap"). 
            Defaults to "Esri.WorldImagery" (no API key).
        rectangle_color (str, optional): Hex color for the bounding box. 
            Defaults to "#FF6A00".
        rectangle_weight (int, optional): Stroke width of the bounding box. 
            Defaults to 2.
        polygon_color (str, optional): Hex color for the actual ROI shape. 
            Defaults to "#1f78b4".
        polygon_weight (int, optional): Stroke width of the ROI shape. 
            Defaults to 2.
        polygon_fill_opacity (float, optional): Transparency of the ROI fill. 
            Defaults to 0.1.
        fit_padding (tuple, optional): Padding around the ROI when fitting the 
            map view. Defaults to (40, 40).
        add_polygon (bool, optional): Whether to render the actual polygon 
            geometry on the map. Defaults to True.
        save_path (str, optional): Local file path to save the interactive 
            HTML map. Defaults to None.

    Returns:
        folium.Map: An interactive map object centered on the ROI.

    Raises:
        ValueError: If the input GeoDataFrame is None, empty, or lacks a 
            defined Coordinate Reference System.
    """
    
    if roi_gdf is None:
        raise ValueError("roi_gdf is None")

    if roi_gdf.empty:
        raise ValueError("roi_gdf is empty")

    if roi_gdf.crs is None:
        raise ValueError("roi_gdf has no CRS defined")

    # Reproject to geographic CRS for Folium
    roi = roi_gdf.to_crs(epsg=epsg)

    # Clean geometries
    roi = roi[~roi.geometry.is_empty]
    roi = roi[roi.geometry.is_valid]

    if roi.empty:
        raise ValueError("ROI geometry is empty or invalid after cleaning")

    if use_dissolve:
        geom = roi.dissolve().geometry.iloc[0]
    else:
        geom = roi.iloc[0].geometry

    # Calculate bounds
    minx, miny, maxx, maxy = geom.bounds
    center_lat = (miny + maxy) / 2.0
    center_lon = (minx + maxx) / 2.0

    # Create map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom_start,
        tiles=tiles,
    )

    bounds_sw = [miny, minx]
    bounds_ne = [maxy, maxx]

    folium.Rectangle(
        bounds=[bounds_sw, bounds_ne],
        color=rectangle_color,
        weight=rectangle_weight,
        fill=False,
        tooltip="ROI bounds",
    ).add_to(m)

    if add_polygon:
        folium.GeoJson(
            mapping(geom),
            name="ROI",
            style_function=lambda _: {
                "color": polygon_color,
                "weight": polygon_weight,
                "fillOpacity": polygon_fill_opacity,
            },
            tooltip="ROI polygon",
        ).add_to(m)

    m.fit_bounds([bounds_sw, bounds_ne], padding=fit_padding)

    if save_path:
        m.save(save_path)

    return m

# ====================================================================================================

def get_roi_wkt_from_vector(file_path, use_bbox: bool = True, simplify_tolerance: float = 0.001) -> str:
    """
    Convert a vector dataset into a WKT geometry (EPSG:4326) for Copernicus API queries.

    This function reads a spatial vector file (e.g., Shapefile, GeoJSON),
    ensures it is in a geographic Coordinate Reference System (EPSG:4326),
    and returns either its overall bounding box or its dissolved, simplified
    polygon as a WKT string for spatial filtering of the Copernicus catalogue.

    Args:
        file_path (str or Path): Path to the input vector dataset defining the ROI.
        use_bbox (bool, optional): If True (default), returns the bounding box of all
            features — short, robust URLs. If False, returns the dissolved polygon,
            simplified by ``simplify_tolerance`` degrees to keep the query URL short.
        simplify_tolerance (float, optional): Simplification tolerance in degrees
            when ``use_bbox=False``. Defaults to 0.001 (~100 m).

    Returns:
        str: A WKT Polygon string in EPSG:4326.

    Raises:
        ValueError: If the input vector file has no defined Coordinate Reference System (CRS).
    """
    gdf = gpd.read_file(file_path)

    if gdf.crs is None:
        raise ValueError("The provided spatial file has no defined CRS.")

    if gdf.crs.to_epsg() != 4326:
        original_crs_name = gdf.crs.name
        gdf = gdf.to_crs(epsg=4326)
        print(f"Reprojecting ROI from {original_crs_name} to {gdf.crs.name}...")

    if use_bbox:
        bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
        return box(bounds[0], bounds[1], bounds[2], bounds[3]).wkt

    geom = _fix_geom(unary_union(gdf.geometry)).simplify(simplify_tolerance, preserve_topology=True)
    if geom.geom_type == "MultiPolygon":
        print("\x1b[93mROI is a MultiPolygon; using its convex hull for the API query.\x1b[0m")
        geom = geom.convex_hull
    return geom.wkt

# ====================================================================================================

def get_keycloak(username: str, password: str) -> str:
    """
    Retrieve an access token from the Copernicus Data Space Ecosystem (CDSE)
    Keycloak identity service (Resource Owner Password Credentials grant).

    Args:
        username (str): The CDSE account username.
        password (str): The CDSE account password.

    Returns:
        str: A JWT access token string used for authenticated API requests.

    Raises:
        Exception: If the authentication request fails due to network issues,
            invalid credentials, or an unexpected server response.
    """
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(CDSE_TOKEN_URL, data=data, timeout=60)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Keycloak token creation failed: {str(e)}")
    return r.json()["access_token"]

# ====================================================================================================

def _fix_geom(geom: BaseGeometry) -> BaseGeometry:
    """
    Repair and validate a single geometry object using multiple fallback strategies.

    This function attempts to correct invalid or corrupted geometries commonly
    encountered in spatial workflows (e.g., self-intersections, ring orientation
    issues, or topology errors). It prioritizes robust validation using
    ``shapely.validation.make_valid`` when available, and falls back to the
    widely used zero-distance buffer trick as a secondary repair method.
    If all repair attempts fail or produce empty geometries, the original
    geometry is returned unchanged.

    Args:
        geom (BaseGeometry): The input Shapely geometry object to validate
            and repair (e.g., Polygon, MultiPolygon, LineString).

    Returns:
        BaseGeometry: A repaired and valid geometry if successful. If the input
        is None, returns None. If all repair strategies fail, returns the
        original geometry.

    Raises:
        None: All exceptions during repair attempts are safely handled to ensure
        the function never breaks execution in batch processing workflows.

    Notes:
        - ``make_valid`` is preferred as it preserves geometry structure more
          reliably (Shapely >= 1.8 / GEOS >= 3.8).
        - ``buffer(0)`` is a legacy fallback that can resolve simple topology
          issues but may alter geometry structure in complex cases.
        - Empty geometries produced during repair are discarded in favour of
          retaining meaningful spatial features.
    """
    if geom is None:
        return None
    try:
        from shapely.validation import make_valid
        g2 = make_valid(geom)
        if g2 is not None and not g2.is_empty:
            return g2
    except Exception:
        pass
    try:
        g3 = geom.buffer(0)
        if g3 is not None and not g3.is_empty:
            return g3
    except Exception:
        pass
    return geom

# ====================================================================================================

def _swap_xy_geom(geom):
    """
    Swap the coordinate order of a geometry from (x, y) to (y, x).

    This function transforms all coordinates within a Shapely geometry by
    reversing their axis order. It is commonly used to correct coordinate
    inconsistencies between systems that interpret spatial data differently
    (e.g., latitude/longitude vs longitude/latitude). The transformation is
    applied consistently across all vertices, preserving the original geometry
    type and structure.

    Args:
        geom (BaseGeometry): The input Shapely geometry whose coordinates
            need to be swapped (e.g., Point, LineString, Polygon, MultiPolygon).

    Returns:
        BaseGeometry: A new geometry with all coordinates swapped. If the input
        is None, returns None.

    Raises:
        None: This function assumes valid input geometry and does not explicitly
        handle transformation errors.

    Notes:
        - Z-coordinates (if present) are preserved and passed through unchanged.
        - Useful when working with data sources that mix axis conventions,
          particularly in CRS transformations or API outputs.
        - Internally uses ``shapely.ops.transform`` for efficient coordinate
          mapping across all geometry types.
    """
    if geom is None:
        return None
    return transform(lambda x, y, z=None: (y, x) if z is None else (y, x, z), geom)

# ====================================================================================================

def _utm_epsg_for(lon, lat):
    """
    Determine the appropriate UTM EPSG code for a given geographic coordinate.

    This function calculates the Universal Transverse Mercator (UTM) zone
    based on a longitude value and assigns the corresponding EPSG code
    depending on the hemisphere defined by the latitude. It enables dynamic
    selection of a projected Coordinate Reference System (CRS) suitable for
    spatial analysis, distance measurement, and raster processing.

    Args:
        lon (float): Longitude in decimal degrees (WGS84).
        lat (float): Latitude in decimal degrees (WGS84).

    Returns:
        int: The EPSG code for the corresponding UTM zone:
            - Northern Hemisphere → EPSG:326XX
            - Southern Hemisphere → EPSG:327XX
            where XX is the UTM zone number (01–60).

    Raises:
        None: Assumes valid numeric latitude and longitude inputs.

    Notes:
        - UTM zones are 6-degree longitudinal bands numbered from 1 to 60.
        - This function does not account for special UTM exceptions
          (e.g., Norway or Svalbard zone adjustments).
        - The returned EPSG is compatible with most GIS and remote sensing
          workflows using projected coordinate systems.
    """
    zone = int((lon + 180) // 6) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone

# ====================================================================================================

def _to_geom(g):
    """
    Convert various geometry representations into a Shapely geometry object.

    This function standardizes heterogeneous geometry inputs commonly returned
    by APIs (e.g., Copernicus/Sentinel metadata) into a consistent Shapely
    geometry. It supports multiple formats, including WKT strings, GeoJSON-like
    dictionaries, and Copernicus-specific GeoFootprint structures. Invalid or
    unrecognized inputs are safely handled to maintain robustness in automated
    workflows.

    Args:
        g (Union[dict, str]): Input geometry representation. Supported formats:
            - Dictionary with a "Wkt" key containing a WKT string
            - GeoJSON-like dictionary with "type" and "coordinates"
            - Raw WKT string

    Returns:
        BaseGeometry: A valid Shapely geometry object if parsing succeeds.
        Returns None if the input is invalid, empty, or cannot be parsed.

    Raises:
        None: All parsing errors are caught internally to prevent workflow
        interruption during batch processing.

    Notes:
        - Uses ``shapely.wkt.loads`` for WKT parsing and
          ``shapely.geometry.shape`` for GeoJSON conversion.
        - Designed for resilience when working with external metadata sources
          that may provide inconsistent geometry formats.
        - Empty or malformed inputs are quietly ignored (return None) to allow
          downstream validation and filtering.
    """
    try:
        if isinstance(g, dict):
            if "Wkt" in g and isinstance(g["Wkt"], str):
                return wkt.loads(g["Wkt"])
            if "type" in g and "coordinates" in g:
                return shp_shape(g)
        if isinstance(g, str) and g.strip():
            return wkt.loads(g)
    except Exception:
        return None
    return None

# ====================================================================================================

def build_query_url(
    data_collection,
    ROI,
    time_begin,
    time_end,
    cloud_max=100,
    page_size=500,
):
    """
    Construct the CDSE OData query URL.

    Filters by collection, spatial intersection with the ROI, sensing date window
    and maximum cloud cover. The end date is **inclusive** (the whole ``time_end``
    day is queried). Attributes are expanded so cloud cover can be reported in the
    selection preview.

    Args:
        data_collection (str): e.g. "SENTINEL-2".
        ROI (str): WKT geometry in EPSG:4326.
        time_begin (str): Start date (YYYY-MM-DD), inclusive.
        time_end (str): End date (YYYY-MM-DD), inclusive.
        cloud_max (float, optional): Maximum cloud cover (0–100). Use None to skip
            the filter. Defaults to 100.
        page_size (int, optional): Products per page (CDSE max 1000). Defaults to 500.

    Returns:
        str: Fully encoded query URL.
    """
    t0 = datetime.strptime(time_begin, "%Y-%m-%d")
    t1 = datetime.strptime(time_end, "%Y-%m-%d") + timedelta(days=1)

    filters = [
        f"Collection/Name eq '{data_collection}'",
        f"OData.CSC.Intersects(area=geography'SRID=4326;{ROI}')",
        f"ContentDate/Start ge {t0:%Y-%m-%d}T00:00:00.000Z",
        f"ContentDate/Start lt {t1:%Y-%m-%d}T00:00:00.000Z",
    ]
    if cloud_max is not None:
        filters.append(
            "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and "
            f"att/OData.CSC.DoubleAttribute/Value le {float(cloud_max)})"
        )

    url = (
        f"{CDSE_ODATA_URL}?$filter={' and '.join(filters)}"
        f"&$orderby=ContentDate/Start asc&$expand=Attributes&$count=True&$top={int(page_size)}"
    )
    return html.unescape(url).replace(" ", "%20")

# ====================================================================================================

def query_copernicus_catalog(
    data_collection,
    ROI,
    time_begin,
    time_end,
    cloud_max,
    cert_path,
    time_out,
    page_size=500,
    max_products=20000,
):
    """
    Query the Copernicus Data Space catalogue for satellite products
    matching spatial, temporal, and quality constraints, following pagination.

    Args:
        data_collection (str): The name of the data collection (e.g., "SENTINEL-2").
        ROI (str): Region of Interest as a WKT geometry string in EPSG:4326.
        time_begin (str): Start date in ISO format (YYYY-MM-DD), inclusive.
        time_end (str): End date in ISO format (YYYY-MM-DD), inclusive.
        cloud_max (float): Maximum allowable cloud cover percentage (0–100).
        cert_path (str or bool): Path to SSL certificate bundle, or True/None for defaults.
        time_out (int or float): Timeout duration (seconds) per HTTP request.
        page_size (int, optional): Products per page. Defaults to 500.
        max_products (int, optional): Safety cap on total products. Defaults to 20000.

    Returns:
        dict: ``{"value": [...], "@odata.count": n}`` with all pages merged, or None
        if the request fails.

    Notes:
        - The end date is inclusive; earlier versions excluded the last day.
        - Follows ``@odata.nextLink`` (with a ``$skip`` fallback), so results are
          no longer capped at 1000 products.
    """
    print("Starting query to Copernicus Data Space...")
    url = build_query_url(data_collection, ROI, time_begin, time_end, cloud_max, page_size)
    print(f"Query URL:\n{url}")

    verify = cert_path if cert_path else True
    records, page, total = [], 0, None
    try:
        while url and len(records) < max_products:
            response = requests.get(url, verify=verify, timeout=time_out)
            response.raise_for_status()
            data = response.json()
            if total is None:
                total = data.get("@odata.count")
                if total is not None:
                    print(f"Catalogue reports {total} matching products.")
            batch = data.get("value", [])
            records.extend(batch)
            page += 1

            next_link = data.get("@odata.nextLink")
            if next_link:
                url = next_link
            elif total is not None and len(records) < total and batch:
                base = re.sub(r"&\$skip=\d+", "", url)
                url = f"{base}&$skip={len(records)}"
            else:
                url = None
        print(f"Retrieved {len(records)} products in {page} page(s).")
        return {"value": records, "@odata.count": total}
    except requests.exceptions.RequestException as e:
        print(f"\x1b[91mError fetching data: {e}\x1b[0m")
        return None

# ====================================================================================================

def _flatten_attributes(attrs) -> dict:
    """Turn the CDSE ``Attributes`` list into a {Name: Value} dict."""
    if not isinstance(attrs, list):
        return {}
    return {a.get("Name"): a.get("Value") for a in attrs if isinstance(a, dict)}


def parse_footprints(json_data):
    """
    Parse Copernicus catalogue JSON response into a GeoDataFrame of product footprints.

    Converts the embedded GeoFootprint information into Shapely geometries
    (EPSG:4326) and adds convenience columns used by the preview and download
    stages: ``identifier`` (name without ``.SAFE``), ``tile`` (MGRS), ``acq_date``,
    ``cloudCover``, ``size_gb``.

    Args:
        json_data (dict): JSON response from ``query_copernicus_catalog``, containing
            a "value" key with a list of products.

    Returns:
        gpd.GeoDataFrame: Product metadata and geometries in EPSG:4326, or None
        if no valid products are found or geometry parsing fails.
    """
    if not json_data or "value" not in json_data or not json_data["value"]:
        print("No products found in the response.")
        return None

    p = pd.DataFrame.from_dict(json_data["value"])
    print(f"Raw product count: {p.shape[0]}")

    try:
        p["geometry"] = p["GeoFootprint"].apply(_to_geom)
    except Exception as e:
        print(f"Error parsing GeoFootprint geometries: {e}")
        return None

    if "Attributes" in p:
        attrs = pd.DataFrame([_flatten_attributes(a) for a in p["Attributes"]], index=p.index)
        for col in ("cloudCover", "platformSerialIdentifier", "relativeOrbitNumber", "tileId"):
            if col in attrs:
                p[col] = attrs[col]
        p = p.drop(columns=["Attributes"])

    p["identifier"] = p["Name"].str.replace(r"\.SAFE$", "", regex=True)
    p["tile"] = p["Name"].str.extract(r"_(T\d{2}[A-Z]{3})_", expand=False)
    p["acq_date"] = pd.to_datetime(p["Name"].str.extract(r"_(\d{8}T\d{6})_", expand=False),
                                   format="%Y%m%dT%H%M%S", errors="coerce").dt.date
    p["size_gb"] = pd.to_numeric(p.get("ContentLength"), errors="coerce") / 1e9

    productDF = gpd.GeoDataFrame(p, geometry="geometry", crs="EPSG:4326")

    print("All available tiles:")
    for name in productDF["Name"]:
        print("\x1b[94m" + name + "\x1b[0m")

    return productDF

# ====================================================================================================

def filter_by_product_type(productDF, product_type):
    """
    Filter a GeoDataFrame of products by specified product type keywords.

    This function subsets a GeoDataFrame based on matching patterns within
    the product "Name" field (e.g., filtering Sentinel-2 products by levels
    such as 'L1C' or 'L2A'). It supports multiple product type filters by
    constructing a combined regular expression, enabling flexible matching
    across naming conventions. If no product type is provided, the original
    dataset is returned unchanged.

    Args:
        productDF (gpd.GeoDataFrame): Input GeoDataFrame containing product
            metadata, including a "Name" column.
        product_type (list[str] or None): List of product type strings to
            filter by (e.g., ['L1C', 'L2A']). If None or empty, no filtering
            is applied.

    Returns:
        gpd.GeoDataFrame: A filtered copy of the input GeoDataFrame containing
        only products whose names match the specified product types.

    Raises:
        None: Assumes the input GeoDataFrame contains a valid "Name" column.

    Notes:
        - Matching is performed using ``str.contains`` with a combined regex
          pattern built from the provided product types.
        - Filtering is case-sensitive by default (consistent with product
          naming conventions).
        - Prints the number of matched products and their names for quick
          inspection and debugging.
        - Returns a copy of the filtered data to avoid modifying the original
          GeoDataFrame.
    """
    if product_type:
        filt_pat = '|'.join(map(re.escape, product_type))
        filtered_productDF = productDF[productDF["Name"].str.contains(filt_pat, na=False)].copy()
    else:
        filtered_productDF = productDF.copy()

    print(f"\nFiltered tiles matching product type {product_type}: {len(filtered_productDF)}")
    for name in filtered_productDF["Name"]:
        print(name)
        
    return filtered_productDF

# ====================================================================================================

def keep_latest_baseline_only(filtered_productDF):
    """
    Retain only the latest processing baseline for each unique Sentinel-2 scene.

    This function parses Sentinel-2 product names to extract key metadata
    components (platform, processing level, acquisition time, baseline version,
    orbit, and tile). It then groups products by a unique scene identifier
    (platform + level + acquisition timestamp + tile) and retains only the
    product with the highest processing baseline within each group.

    Args:
        filtered_productDF (gpd.GeoDataFrame): Input GeoDataFrame containing
            product metadata with a "Name" column following Sentinel-2 naming
            conventions.

    Returns:
        gpd.GeoDataFrame: A reduced GeoDataFrame containing only the most recent
        baseline product for each unique scene/tile combination.

    Raises:
        None: Assumes valid Sentinel-2 naming format; non-matching names will
        result in missing parsed fields but will not break execution.

    Notes:
        - Uses regex parsing to extract structured metadata from product names
          (e.g., S2A_MSIL2A_YYYYMMDDTHHMMSS_NXXXX_RXXX_TXXXXX_YYYYMMDDTHHMMSS.SAFE).
        - Baseline versions (e.g., N0400) are converted to numeric form to allow
          proper sorting and comparison.
        - Scene grouping is based on platform, processing level, acquisition
          time, and tile ID to ensure uniqueness.
        - Sorting ensures the highest baseline is retained, with product name
          used as a secondary tie-breaker.
        - Designed to remove outdated reprocessed products while preserving
          the most up-to-date dataset for analysis.
    """
    pat = re.compile(
        r'^(S2[ABC])_(MSIL[12][AC])_(\d{8}T\d{6})_(N\d{4})_(R\d{3})_(T[0-9A-Z]{5})_(\d{8}T\d{6})\.SAFE$',
        re.IGNORECASE
    )
    parts = filtered_productDF["Name"].str.extract(pat)
    parts.columns = ["platform", "level", "acq", "baseline", "orbit", "tile", "pkg"]

    df_assigned = filtered_productDF.assign(
        platform=parts["platform"].str.upper(),
        level=parts["level"].str.upper(),
        acq=parts["acq"],
        baseline=parts["baseline"].str.upper(),
        orbit=parts["orbit"].str.upper(),
        tile=parts["tile"].str.upper(),
        pkg=parts["pkg"]
    )

    df_assigned["baseline_num"] = df_assigned["baseline"].str.extract(r'N(\d{4})', expand=False).astype('Int64').fillna(-1)
    df_assigned["scene_key_tile"] = (
        df_assigned["platform"].fillna('') + "_" +
        df_assigned["level"].fillna('') + "_" +
        df_assigned["acq"].fillna('') + "_" +
        df_assigned["tile"].fillna('')
    )

    return (
        df_assigned.sort_values(by=["scene_key_tile", "baseline_num", "Name"], ascending=[True, False, False])
        .groupby("scene_key_tile", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

# ====================================================================================================

def apply_overlap_reduction(
    tiles_to_download,
    ROI,
    min_roi_coverage_frac,
    coverage_mode="acquisition",
    min_new_coverage_frac=0.01,
):
    """
    Reduce overlapping satellite tiles by retaining only those that sufficiently cover a Region of Interest (ROI).

    Two modes are available via ``coverage_mode``:

    - ``"acquisition"`` (default): tiles of one acquisition (same platform, level and
      sensing time) are combined. Tiles are added greedily, largest ROI overlap first,
      and a tile is kept only if it adds at least ``min_new_coverage_frac`` of *new*
      ROI area (so the ~10 km MGRS overlap duplicates are dropped). The acquisition is
      kept if the union of its kept tiles covers ``>= min_roi_coverage_frac`` of the ROI.
      Works for ROIs that are small (one tile is selected) or span several tiles.
    - ``"best_tile"``: the original behaviour — keep the single best tile per
      acquisition, and only if that one tile alone covers ``>= min_roi_coverage_frac``.

    Args (additional):
        coverage_mode (str, optional): "acquisition" (default) or "best_tile".
        min_new_coverage_frac (float, optional): Minimum fraction (0–1) of the ROI a tile
            must newly add to be kept in "acquisition" mode. Defaults to 0.01 (1%).

    This function evaluates the spatial overlap between candidate tile footprints
    and a given ROI, then filters tiles based on a minimum coverage threshold.
    It performs robust geometry handling, including validation, coordinate axis
    correction (lat/lon vs lon/lat), and accurate area calculation in an
    appropriate local UTM projection. For each acquisition (date-based grouping),
    only the best tile (highest ROI coverage) meeting the threshold is retained.

    Args:
        tiles_to_download (gpd.GeoDataFrame): Input GeoDataFrame of candidate
            tiles with valid "Name" and "geometry" columns in EPSG:4326.
        ROI (str): Region of Interest expressed as a WKT geometry string
            in EPSG:4326.
        min_roi_coverage_frac (float): Minimum fraction (0–1) of ROI area that
            a tile must cover to be considered valid.

    Returns:
        gpd.GeoDataFrame: A filtered GeoDataFrame containing one best tile per
        acquisition that satisfies the minimum ROI coverage threshold. Returns
        an empty GeoDataFrame if no tiles meet the criteria.

    Raises:
        None: All geometry parsing and processing errors are handled internally
        to maintain robustness in automated workflows.

    Notes:
        - Uses ``_fix_geom`` to repair invalid geometries before intersection.
        - Automatically detects and corrects axis order issues (lat/lon vs lon/lat)
          in both ROI and tile geometries.
        - Initial overlap checks are done in geographic space (degrees) for quick
          validation, followed by precise area calculations in a local UTM CRS.
        - UTM zone is dynamically selected using the ROI centroid via
          ``_utm_epsg_for`` for accurate area computation in square metres.
        - Tiles are grouped by acquisition (platform, level, timestamp) while
          ignoring tile ID to retain the best spatial coverage per scene.
        - Coverage metrics include absolute intersection area (m²) and fractional
          ROI coverage, both used for ranking.
        - Tiles below the specified coverage threshold are discarded, and entire
          acquisitions may be removed if no tile meets the criteria.
        - Diagnostic print statements provide visibility into per-tile coverage,
          dropped acquisitions, and final selections for transparency/debugging.
    """
    try:
        roi_geom_raw = wkt.loads(ROI)
    except Exception as e:
        print(f"Warning: Failed to parse ROI WKT: {e}")
        return tiles_to_download  # Skip reduction on failure

    if roi_geom_raw is None or tiles_to_download.empty:
        return tiles_to_download

    # Helper for quick geographic overlap detection
    def _apply_overlap_geog(gdf, roi):
        gdf = gdf.copy()
        gdf["__deg_intersect_area"] = gdf.geometry.apply(
            lambda g: _fix_geom(g).intersection(_fix_geom(roi)).buffer(0).area if g is not None else 0.0
        )
        return gdf

    roi_used = _fix_geom(roi_geom_raw)
    tiles_used = tiles_to_download.copy()
    tiles_try = _apply_overlap_geog(tiles_used, roi_used)
    total_area = float(tiles_try["__deg_intersect_area"].sum())

    # Auto-detect and swap lat/lon axis issues
    if total_area == 0.0:
        roi_swapped = _fix_geom(_swap_xy_geom(roi_used))
        tiles_try2 = _apply_overlap_geog(tiles_used, roi_swapped)
        total_area2 = float(tiles_try2["__deg_intersect_area"].sum())
        if total_area2 > 0.0:
            print("Note: ROI appeared to be lat/long. Swapped to long/lat for overlap reduction.")
            roi_used = roi_swapped
            tiles_try = tiles_try2

    if total_area == 0.0:
        tiles_swapped = tiles_used.copy()
        tiles_swapped["geometry"] = tiles_swapped["geometry"].apply(
            lambda g: _fix_geom(_swap_xy_geom(g)) if g is not None else None
        )
        tiles_try3 = _apply_overlap_geog(tiles_swapped, roi_used)
        total_area3 = float(tiles_try3["__deg_intersect_area"].sum())
        if total_area3 > 0.0:
            print("Note: Tile footprints appeared to be lat/long. Swapped to long/lat for overlap reduction.")
            tiles_used = tiles_swapped
            tiles_try = tiles_try3

    # Grouping by Acquisition (ignoring tile)
    pat_acq = re.compile(r'^(S2[ABC])_(MSIL[12][AC])_(\d{8}T\d{6})_', re.IGNORECASE)
    def parse_acq_key(name):
        m = pat_acq.match(name)
        if m:
            return (m.group(1).upper(), m.group(2).upper(), m.group(3))
        return ("UNK", "UNK", "UNK")
        
    tiles_used["__acq_group"] = tiles_used["Name"].apply(parse_acq_key)

    # Compute area accurately in local UTM
    cx, cy = roi_used.centroid.x, roi_used.centroid.y
    utm_epsg = _utm_epsg_for(cx, cy)
    proj_fn = Transformer.from_crs("EPSG:4326", f"EPSG:{utm_epsg}", always_xy=True).transform

    roi_fixed = _fix_geom(roi_used)
    roi_proj = transform(proj_fn, roi_fixed)
    roi_area_m2 = roi_proj.area

    if roi_area_m2 <= 0:
        print("Warning: ROI projected area invalid (<=0). Nothing will be downloaded.")
        return tiles_used.iloc[0:0].copy()

    def inter_area_frac(geom):
        if geom is None:
            return 0.0, 0.0
        gfix = _fix_geom(geom)
        inter = _fix_geom(gfix.intersection(roi_fixed))
        if inter.is_empty:
            return 0.0, 0.0
        inter_proj = transform(proj_fn, inter)
        a = inter_proj.area
        return a, a / roi_area_m2

    tiles_used[["__inter_m2", "__frac"]] = tiles_used["geometry"].apply(
        lambda geom: pd.Series(inter_area_frac(geom))
    )

    thr = float(min_roi_coverage_frac)
    kept_rows = []
    dropped_acqs = 0

    if coverage_mode == "best_tile":
        if len(tiles_used) > 0:
            print("\nPer-tile ROI coverage (before thresholding):")
            for _, r in tiles_used.sort_values("__frac", ascending=False).head(30).iterrows():
                print(f"  {r['Name']} -> {r['__frac']*100:.2f}% of ROI")
            if len(tiles_used) > 30:
                print(f"  ... ({len(tiles_used) - 30} more)")

        # Retain best tile per acquisition above threshold
        for acq, g in tiles_used.groupby("__acq_group", dropna=False):
            g_ok = g[g["__frac"] >= thr].copy()
            if g_ok.empty:
                dropped_acqs += 1
                continue
            best = g_ok.sort_values(by=["__frac", "__inter_m2", "Name"], ascending=[False, False, True]).head(1)
            kept_rows.append(best)

    elif coverage_mode == "acquisition":
        print(f"\nPer-acquisition ROI coverage (threshold {thr*100:.0f}%, tiles must add >= {min_new_coverage_frac*100:.1f}% new ROI area):")
        for acq, g in sorted(tiles_used.groupby("__acq_group", dropna=False), key=lambda kv: str(kv[0][2])):
            g = g.sort_values(by=["__frac", "Name"], ascending=[False, True])
            covered = None
            keep_idx = []
            for idx, r in g.iterrows():
                if r["__frac"] <= 0 or r.geometry is None:
                    continue
                inter = _fix_geom(_fix_geom(r.geometry).intersection(roi_fixed))
                if inter.is_empty:
                    continue
                new_part = inter if covered is None else _fix_geom(inter.difference(covered))
                new_frac = 0.0 if new_part.is_empty else transform(proj_fn, new_part).area / roi_area_m2
                if new_frac >= float(min_new_coverage_frac):
                    keep_idx.append(idx)
                    covered = inter if covered is None else _fix_geom(unary_union([covered, inter]))
            acq_frac = 0.0 if covered is None else transform(proj_fn, covered).area / roi_area_m2
            ok = bool(keep_idx) and acq_frac >= thr
            flag = "\x1b[92m✓\x1b[0m" if ok else "\x1b[91m✗\x1b[0m"
            print(f"  {flag} {acq[0]} {acq[1]} {acq[2]}  tiles kept={len(keep_idx)}/{len(g)}  coverage={acq_frac*100:.1f}%")
            if not ok:
                dropped_acqs += 1
                continue
            sel = g.loc[keep_idx].copy()
            sel["acq_cover_pct"] = acq_frac * 100.0
            kept_rows.append(sel)
    else:
        raise ValueError(f"Unknown coverage_mode '{coverage_mode}'. Use 'acquisition' or 'best_tile'.")

    kept = pd.concat(kept_rows, ignore_index=True) if kept_rows else tiles_used.iloc[0:0].copy()

    if dropped_acqs > 0:
        print(f"Acquisitions dropped by min_roi_coverage_frac={min_roi_coverage_frac}: {dropped_acqs}")

    if not kept.empty:
        print(f"\nTiles selected (min_roi_coverage_frac >= {min_roi_coverage_frac:.2f}, mode='{coverage_mode}'):")
        for _, row in kept.iterrows():
            print(f"  {row['Name']} -> ROI coverage: {row['__frac']*100:.2f}%")

    if not kept.empty:
        kept["roi_cover_pct"] = kept["__frac"] * 100.0
    return kept.drop(columns=["__deg_intersect_area", "__acq_group", "__inter_m2", "__frac", "scene_key_tile"], errors="ignore").copy()

# ====================================================================================================

@contextmanager
def download_timer(process_name="Process"):
    """
    Measure and report the execution time of a code block using a context manager.

    This utility provides a simple and consistent way to profile runtime for
    sections of code. It records the start and end timestamps, prints readable
    timing information, and outputs the total elapsed duration in a human-friendly
    format (seconds, minutes, or hours). Designed for quick diagnostics,
    performance monitoring, and logging within data processing pipelines.

    Args:
        process_name (str, optional): Descriptive name of the process being timed,
            used in console output for clarity. Defaults to "Process".

    Yields:
        None: Control is passed to the wrapped block inside the ``with`` statement.

    Raises:
        None: Ensures timing output is always printed, even if an exception
        occurs within the wrapped block.

    Notes:
        - Uses ``time.time()`` for wall-clock timing.
        - Outputs both start/end timestamps and total elapsed duration.
        - Applies ANSI color formatting for improved readability in terminal output.
        - Suitable for debugging, benchmarking, and monitoring long-running
          geospatial or data processing workflows.
        - Example usage:
            >>> with Timer("Download Pipeline"):
            >>>     run_pipeline()
    """
    start_time = time.time()
    start_time_readable = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
    
    print(f"\n--- Starting {process_name} ---")
    print(f"Start time: {start_time_readable}\n")
    
    try:
        # Yield control back to the block of code inside the 'with' statement
        yield
    finally:
        end_time = time.time()
        end_time_readable = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
        
        elapsed_time = end_time - start_time
        elapsed_hours = int(elapsed_time // 3600)
        elapsed_minutes = int((elapsed_time % 3600) // 60)
        elapsed_seconds = int(elapsed_time % 60)

        print(f"\n--- {process_name} Complete ---")
        print("\x1b[94m" + f"Start time: {start_time_readable}" + "\x1b[0m")
        print("\x1b[94m" + f"End time:   {end_time_readable}" + "\x1b[0m")
        
        # Format the elapsed time nicely
        if elapsed_hours > 0:
            print("\x1b[92m" + f"Elapsed time: {elapsed_hours}h {elapsed_minutes}m {elapsed_seconds}s" + "\x1b[0m")
        elif elapsed_minutes > 0:
            print("\x1b[92m" + f"Elapsed time: {elapsed_minutes}m {elapsed_seconds}s" + "\x1b[0m")
        else:
            print("\x1b[92m" + f"Elapsed time: {elapsed_seconds}s" + "\x1b[0m")

# ====================================================================================================

# ====================================================================================================
# SEARCH (query + parse + filter) — shared by preview, download and resume
# ====================================================================================================

def search_copernicus_products(
    data_collection,
    ROI,
    time_begin,
    time_end,
    product_type=None,
    download_latest_only=False,
    enable_overlap_reduction=True,
    min_roi_coverage_frac=0.75,
    cloud_max=100,
    coverage_mode="acquisition",
    min_new_coverage_frac=0.01,
    cert_path=None,
    time_out=2400,
    **_ignored,
):
    """
    Run the full selection chain without downloading anything.

    Stages: catalogue query (spatial + temporal + cloud) → footprint parsing →
    product-type filter → optional latest-baseline filter → optional overlap
    reduction. Accepts (and ignores) extra keyword arguments so the same
    ``pipeline_config`` dictionary used for downloading can be passed straight in.

    Returns:
        gpd.GeoDataFrame | None: Products selected for download (empty if none),
        or None if the catalogue query failed.
    """
    if cert_path is None and _certifi is not None:
        cert_path = _certifi.where()

    # 1. Fetch JSON manifest
    json_data = query_copernicus_catalog(
        data_collection, ROI, time_begin, time_end, cloud_max, cert_path, min(int(time_out), 600)
    )
    if json_data is None:
        return None

    empty = gpd.GeoDataFrame(columns=["Name", "Id", "identifier", "geometry"], geometry="geometry", crs="EPSG:4326")

    # 2. Extract Footprints into GeoDataframe
    productDF = parse_footprints(json_data)
    if productDF is None or productDF.empty:
        return empty

    # 3. Filter by product type flag ('L1C', 'L2A', etc.)
    filtered_productDF = filter_by_product_type(productDF, product_type)
    if filtered_productDF.empty:
        print("No tiles matched the specified product type.")
        return empty

    # 4. Optional "latest baseline" filter
    if download_latest_only:
        tiles = keep_latest_baseline_only(filtered_productDF)
    else:
        tiles = filtered_productDF.copy()

    # 5. Overlap reduction thresholds
    if enable_overlap_reduction:
        tiles = apply_overlap_reduction(tiles, ROI, min_roi_coverage_frac, coverage_mode, min_new_coverage_frac)
    else:
        print("Skipping overlap reduction based on user preference.")

    tiles = tiles.drop(columns=["scene_key_tile"], errors="ignore")
    if not isinstance(tiles, gpd.GeoDataFrame):
        tiles = gpd.GeoDataFrame(tiles, geometry="geometry", crs="EPSG:4326")
    return tiles.reset_index(drop=True)

# ====================================================================================================
# REPORTING
# ====================================================================================================

def summarise_products(productDF) -> pd.DataFrame:
    """
    Tabular summary of selected products, with total volume printed.

    Args:
        productDF (gpd.GeoDataFrame): Selected products.

    Returns:
        pd.DataFrame: One row per product (date, platform, level, tile, baseline,
        cloud cover, ROI coverage, size, name).
    """
    if productDF is None or productDF.empty:
        print("No products to summarise.")
        return pd.DataFrame()

    cols = {
        "acq_date": "Date", "platform": "Platform", "level": "Level", "tile": "Tile",
        "baseline": "Baseline", "cloudCover": "Cloud %", "roi_cover_pct": "ROI cover %",
        "acq_cover_pct": "Acq. cover %",
        "size_gb": "Size (GB)", "Name": "Name",
    }
    present = [c for c in cols if c in productDF.columns]
    out = pd.DataFrame(productDF[present]).rename(columns=cols).sort_values(["Date", "Name"]).reset_index(drop=True)
    for c in ("Cloud %", "ROI cover %", "Acq. cover %", "Size (GB)"):
        if c in out:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(2)

    total_gb = pd.to_numeric(productDF.get("size_gb"), errors="coerce").sum()
    print(f"\x1b[94m{len(out)} products  |  {out['Date'].nunique()} acquisition dates  |  "
          f"total ≈ {total_gb:.1f} GB\x1b[0m")
    return out

# ====================================================================================================

def visualise_footprints(
    productDF,
    ROI: str,
    tiles: str = "Esri.WorldImagery",
    save_path: Optional[str] = None,
) -> folium.Map:
    """
    Map the selected product footprints against the ROI, coloured by relative orbit.

    Sentinel-2 tiles sit on a fixed MGRS grid, so repeat acquisitions of the same
    tile from the same relative orbit have (near-)identical footprints. These are
    merged into **one outline per tile + relative orbit**, coloured by orbit, with a
    tooltip listing every date and its cloud cover. Footprints from a relative orbit
    whose swath edge crosses the tile appear as partial (diagonal-edged) polygons.
    The layer control (top right) doubles as a legend and lets each orbit be toggled.

    Args:
        productDF (gpd.GeoDataFrame): Selected products.
        ROI (str): WKT geometry in EPSG:4326.
        tiles (str, optional): Basemap. Defaults to "Esri.WorldImagery" (no API key).
        save_path (str, optional): Save as standalone HTML.

    Returns:
        folium.Map
    """
    palette = ["#00e5ff", "#ffb000", "#ff3df5", "#7CFC00", "#ff6f3c", "#b388ff", "#ffffff", "#00c853"]

    roi_geom = wkt.loads(ROI)
    minx, miny, maxx, maxy = roi_geom.bounds
    m = folium.Map(location=[(miny + maxy) / 2, (minx + maxx) / 2], zoom_start=8, tiles=tiles)

    if productDF is not None and not productDF.empty:
        df = productDF.copy()
        df["__rel"] = df["Name"].str.extract(r"_(R\d{3})_", expand=False).fillna("R???")
        df["__tile"] = df.get("tile", df["Name"].str.extract(r"_(T\d{2}[A-Z]{3})_", expand=False)).fillna("T?????")
        orbits = sorted(df["__rel"].unique())
        colours = {o: palette[i % len(palette)] for i, o in enumerate(orbits)}

        for orbit in orbits:
            sub = df[df["__rel"] == orbit]
            label = (f'<span style="color:{colours[orbit]};font-size:16px">&#9632;</span> '
                     f'Orbit {orbit} — {len(sub)} scene{"s" if len(sub) != 1 else ""}')
            fg = folium.FeatureGroup(name=label, show=True)
            for tile_id, g in sub.groupby("__tile"):
                geom = _fix_geom(unary_union([x for x in g.geometry if x is not None]))
                if geom is None or geom.is_empty:
                    continue
                lines = []
                for _, r in g.sort_values("acq_date" if "acq_date" in g else "Name").iterrows():
                    cc = r.get("cloudCover")
                    cc_txt = f"{float(cc):.1f}% cloud" if cc is not None and pd.notna(cc) else "cloud n/a"
                    sz = r.get("size_gb")
                    sz_txt = f" · {float(sz):.2f} GB" if sz is not None and pd.notna(sz) else ""
                    lines.append(f"{r.get('acq_date')} · {r.get('platform', r['Name'][:3])} · {cc_txt}{sz_txt}")
                tip = (f"<b>{tile_id} · orbit {orbit}</b> ({len(g)} scene{'s' if len(g) != 1 else ''})<br>"
                       + "<br>".join(lines))
                folium.GeoJson(
                    mapping(geom),
                    style_function=lambda _, c=colours[orbit]: {"color": c, "weight": 2.5, "fillColor": c,
                                                                "fillOpacity": 0.08, "dashArray": None},
                    highlight_function=lambda _: {"weight": 4, "fillOpacity": 0.2},
                    tooltip=folium.Tooltip(tip, sticky=True),
                ).add_to(fg)
            fg.add_to(m)

        b = df.total_bounds
        m.fit_bounds([[min(b[1], miny), min(b[0], minx)], [max(b[3], maxy), max(b[2], maxx)]], padding=(20, 20))

    folium.GeoJson(
        mapping(roi_geom), name='<span style="color:#e31a1c;font-size:16px">&#9632;</span> ROI',
        style_function=lambda _: {"color": "#e31a1c", "weight": 3, "fillOpacity": 0.15},
        tooltip="ROI",
    ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    if save_path:
        m.save(save_path)
    return m

# ====================================================================================================
# DOWNLOADS
# ====================================================================================================

class _TokenManager:
    """Thread-safe Keycloak token cache that refreshes once older than ``TOKEN_MAX_AGE_S``."""

    def __init__(self, user, password):
        self._user, self._password = user, password
        self._token, self._issued = None, 0.0
        self._lock = Lock()

    def get(self, force: bool = False) -> str:
        with self._lock:
            if force or self._token is None or (time.time() - self._issued) > TOKEN_MAX_AGE_S:
                self._token = get_keycloak(self._user, self._password)
                self._issued = time.time()
            return self._token


def _format_elapsed(seconds: float) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h:02}:{m:02}:{s:02}"


def _download_single_product(feat: dict, save_folder: str, tokens: _TokenManager, cert_path, time_out) -> tuple:
    """
    Download one product to ``<identifier>.zip`` via a ``.part`` file.

    Returns:
        tuple(str, bool, str): (product Id, success flag, message)
    """
    name, product_id, identifier = feat["Name"], feat["Id"], feat["identifier"]
    expected = feat.get("ContentLength")
    expected = int(expected) if expected is not None and pd.notna(expected) and expected else None
    file_path = os.path.join(save_folder, f"{identifier}.zip")
    part_path = file_path + ".part"
    verify = cert_path if cert_path else True

    # Skip if a complete file already exists
    if os.path.exists(file_path) and (expected is None or os.path.getsize(file_path) == expected):
        return product_id, True, f"Already present, skipped: {name}"

    url = f"{CDSE_ODATA_URL}({product_id})/$value"
    start = time.time()

    for attempt in (1, 2):
        try:
            with requests.Session() as session:
                session.headers.update({"Authorization": f"Bearer {tokens.get(force=(attempt == 2))}"})

                # CDSE redirects to the download node; follow manually so the
                # Authorization header is retained.
                resp = session.get(url, allow_redirects=False, verify=verify, timeout=time_out, stream=True)
                hops = 0
                while resp.status_code in (301, 302, 303, 307, 308) and "Location" in resp.headers and hops < 5:
                    resp = session.get(resp.headers["Location"], allow_redirects=False, verify=verify,
                                       timeout=time_out, stream=True)
                    hops += 1

                if resp.status_code == 401 and attempt == 1:
                    continue  # refresh token and retry once
                if resp.status_code != 200:
                    msg = f"HTTP {resp.status_code}"
                    try:
                        msg += f" {resp.json()}"
                    except Exception:
                        pass
                    return product_id, False, f"Failed {name}: {msg}"

                ctype = resp.headers.get("Content-Type", "")
                if not any(t in ctype for t in ("zip", "octet-stream")):
                    return product_id, False, f"Failed {name}: unexpected content type '{ctype}'"

                written, last_tick = 0, time.time()
                with open(part_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            written += len(chunk)
                            if time.time() - last_tick >= 10:
                                pct = f" {100 * written / expected:5.1f}%" if expected else ""
                                sys.stdout.write(f"\r  {identifier[:40]}…{pct} {written / 1e9:.2f} GB "
                                                 f"[{_format_elapsed(time.time() - start)}]")
                                sys.stdout.flush()
                                last_tick = time.time()

            if expected is not None and written != expected:
                os.remove(part_path)
                return product_id, False, f"Failed {name}: size mismatch ({written} vs {expected} bytes)"

            os.replace(part_path, file_path)
            return product_id, True, f"Downloaded: {name} ({written / 1e9:.2f} GB) in {_format_elapsed(time.time() - start)}"

        except Exception as e:
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except OSError:
                    pass
            if attempt == 2:
                return product_id, False, f"Error downloading {name}: {e}"

    return product_id, False, f"Failed {name}: authentication retry exhausted"

# ====================================================================================================

def execute_downloads(
    tiles_to_download,
    save_folder,
    copernicus_user,
    copernicus_password,
    max_parallel_downloads,
    cert_path,
    time_out,
    log_csv_path=None,
):
    """
    Execute authenticated downloads of Copernicus products (sequentially or in parallel).

    Handles Keycloak authentication with age-based token refresh, manual redirect
    following to the storage endpoint, streamed writes to ``<identifier>.zip.part``
    with byte-size verification against the catalogue ``ContentLength``, and skipping
    of files already present. Each success is appended to ``log_csv_path`` as soon
    as it completes, so an interrupted run loses at most the files in flight.

    Args:
        tiles_to_download (gpd.GeoDataFrame): Products with "Name", "Id", "identifier"
            (and optionally "ContentLength") fields.
        save_folder (str): Local directory for the ZIP files.
        copernicus_user (str): CDSE username.
        copernicus_password (str): CDSE password.
        max_parallel_downloads (int): Number of workers (1 = sequential).
        cert_path (str or bool): SSL certificate bundle path.
        time_out (int or float): HTTP timeout (seconds).
        log_csv_path (str, optional): CSV log that successful product Ids are appended to.

    Returns:
        tuple:
            - list[str]: Product IDs successfully downloaded.
            - list[str]: Product names that failed.
    """
    tokens = _TokenManager(copernicus_user, copernicus_password)
    print("Authenticating with Copernicus Keycloak...")
    tokens.get()
    print("Token retrieved successfully.")

    downloaded, failed = [], []
    lock = Lock()
    records = tiles_to_download.to_dict("records")

    def _record(result, name):
        pid, ok, msg = result
        with lock:
            print(("\n\x1b[92m" if ok else "\n\x1b[91m") + msg + "\x1b[0m")
            if ok:
                downloaded.append(pid)
                if log_csv_path:
                    with open(log_csv_path, "a") as fh:
                        fh.write(f"{pid}\n")
            else:
                failed.append(name)

    workers = int(max(1, max_parallel_downloads))
    if workers == 1:
        for rec in tqdm(records, total=len(records)):
            _record(_download_single_product(rec, save_folder, tokens, cert_path, time_out), rec["Name"])
    else:
        print(f"\nParallel downloads enabled: {workers} workers")
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_download_single_product, r, save_folder, tokens, cert_path, time_out): r["Name"]
                       for r in records}
            for fut in tqdm(as_completed(futures), total=len(futures)):
                _record(fut.result(), futures[fut])

    return downloaded, failed

# ====================================================================================================
# END-TO-END PIPELINES
# ====================================================================================================

def _default_log_path(save_folder, time_begin, time_end):
    return os.path.join(save_folder, f"log_{time_begin.replace('-', '_')}_to_{time_end.replace('-', '_')}.csv")


def download_copernicus_data(
    data_collection,
    ROI,
    time_begin,
    time_end,
    copernicus_user,
    copernicus_password,
    product_type,
    save_folder,
    time_out=2400,
    download_latest_only=False,
    enable_overlap_reduction=True,
    cert_path=None,
    max_parallel_downloads=1,
    min_roi_coverage_frac=0.75,
    cloud_max=100,
    coverage_mode="acquisition",
    min_new_coverage_frac=0.01,
    log_csv_path=None,
    skip_logged=False,
):
    """
    End-to-end pipeline for querying, filtering, and downloading Copernicus satellite data.

    Stages: catalogue query (spatial + temporal + cloud, paginated) → footprint parsing →
    product-type filter → optional latest-baseline filter → optional overlap reduction →
    summary → authenticated downloads (sequential or parallel) with live logging.

    Args:
        data_collection (str): e.g. "SENTINEL-2".
        ROI (str): WKT geometry string in EPSG:4326.
        time_begin (str): Start date (YYYY-MM-DD), inclusive.
        time_end (str): End date (YYYY-MM-DD), inclusive.
        copernicus_user (str): CDSE username.
        copernicus_password (str): CDSE password.
        product_type (list[str] or None): e.g. ['L1C'], ['L2A'].
        save_folder (str): Directory for downloaded files and the CSV log.
        time_out (int or float, optional): HTTP timeout in seconds. Defaults to 2400.
        download_latest_only (bool, optional): Keep only the newest processing baseline.
        enable_overlap_reduction (bool, optional): Apply the ROI coverage threshold.
        cert_path (str or None, optional): SSL bundle (defaults to certifi).
        max_parallel_downloads (int, optional): Worker threads. Defaults to 1.
        min_roi_coverage_frac (float, optional): Minimum ROI coverage (0–1) per acquisition
            ("acquisition" mode) or per single tile ("best_tile" mode).
        coverage_mode (str, optional): "acquisition" (default) or "best_tile".
        min_new_coverage_frac (float, optional): New ROI area a tile must add to be kept
            in "acquisition" mode. Defaults to 0.01.
        cloud_max (float, optional): Maximum cloud cover percentage. Defaults to 100.
        log_csv_path (str, optional): Log of successful product Ids. Defaults to
            ``<save_folder>/log_<begin>_to_<end>.csv``.
        skip_logged (bool, optional): Skip products already in the log (resume mode).

    Returns:
        list: Product IDs downloaded in this run.
    """
    if cert_path is None and _certifi is not None:
        cert_path = _certifi.where()

    os.makedirs(save_folder, exist_ok=True)
    log_csv_path = log_csv_path or _default_log_path(save_folder, time_begin, time_end)

    tiles_to_download = search_copernicus_products(
        data_collection=data_collection, ROI=ROI, time_begin=time_begin, time_end=time_end,
        product_type=product_type, download_latest_only=download_latest_only,
        enable_overlap_reduction=enable_overlap_reduction, min_roi_coverage_frac=min_roi_coverage_frac,
        cloud_max=cloud_max, coverage_mode=coverage_mode, min_new_coverage_frac=min_new_coverage_frac,
        cert_path=cert_path, time_out=time_out,
    )
    if tiles_to_download is None:
        print("\x1b[91mCatalogue query failed; nothing downloaded.\x1b[0m")
        return []

    if skip_logged:
        if os.path.exists(log_csv_path) and os.path.getsize(log_csv_path) > 0:
            logged = set(pd.read_csv(log_csv_path, header=None)[0].astype(str))
            before = len(tiles_to_download)
            tiles_to_download = tiles_to_download[~tiles_to_download["Id"].astype(str).isin(logged)]
            print(f"\x1b[92mSkipped {before - len(tiles_to_download)} tiles already in log: {log_csv_path}\x1b[0m")
        else:
            print(f"\x1b[93mLog file not found/empty at {log_csv_path}. Proceeding with all tiles.\x1b[0m")

    if tiles_to_download.empty:
        print("No tiles to download.")
        return []

    print(f"\nDownloading {'latest' if download_latest_only else 'all'} filtered tiles: {len(tiles_to_download)}")
    summarise_products(tiles_to_download)
    for name in tiles_to_download["Name"]:
        print("\x1b[92m" + name + "\x1b[0m")

    downloaded_files, failed_downloads = execute_downloads(
        tiles_to_download, save_folder, copernicus_user, copernicus_password,
        max_parallel_downloads, cert_path, time_out, log_csv_path=log_csv_path,
    )

    print(f"\nLog file: {log_csv_path}")
    if not downloaded_files:
        print('\x1b[93mNo data was successfully downloaded.\x1b[0m')
    if failed_downloads:
        print("\n\x1b[91mFailed downloads:\x1b[0m")
        for name in failed_downloads:
            print(name)
    return downloaded_files

# ====================================================================================================

def resume_copernicus_data_download(log_csv_path=None, **pipeline_config):
    """
    Resume an interrupted download, skipping product UUIDs already recorded in the CSV log.

    Takes the same ``pipeline_config`` as ``download_copernicus_data``. New successes
    are appended to the existing log (never overwritten).

    Returns:
        list: Product IDs downloaded in this run.
    """
    return download_copernicus_data(**pipeline_config, log_csv_path=log_csv_path, skip_logged=True)
