"""
Sentinel-2 Download Pipeline Workflow

A robust, automated pipeline for querying, filtering, and downloading Sentinel-2 
satellite imagery from the Copernicus Data Space Ecosystem (CDSE). 

Key Features:
- OData API querying with spatial, temporal, and cloud-cover filtering.
- Automated Keycloak authentication and token management.
- Dynamic spatial overlap reduction using local UTM coordinate reprojection.
- Multithreaded downloading for large-scale datasets.

Author: Julian Manning
Created: April 2026
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
from shapely.ops import transform
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
    tiles: str = "OpenStreetMap",
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
        tiles (str, optional): The basemap provider (e.g., "OpenStreetMap"). 
            Defaults to "OpenStreetMap".
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

def get_roi_wkt_from_vector(file_path) -> str:
    """
    Convert a vector dataset into a WKT bounding box for Copernicus API queries.

    This function reads a spatial vector file (e.g., Shapefile, GeoJSON),
    ensures it is in a geographic Coordinate Reference System (EPSG:4326),
    and extracts the overall bounding box as a WKT polygon string. This
    format is required for spatial filtering when querying the Copernicus
    Data Space catalogue.

    Args:
        file_path (str or Path): Path to the input vector dataset defining
            the Region of Interest (ROI).

    Returns:
        str: A WKT Polygon string representing the bounding box of the
        input dataset in EPSG:4326.

    Raises:
        ValueError: If the input vector file has no defined Coordinate
        Reference System (CRS).

    Notes:
        - Automatically reprojects input data to EPSG:4326 if required.
        - Uses the total bounds of all features to create a rectangular ROI,
          ensuring compatibility with API spatial filters.
        - Internally relies on GeoPandas for I/O and CRS handling, and
          Shapely for geometry construction.
        - The output is a simplified bounding box, not the exact geometry
          of the input features.
    """
    
    # Read the spatial file
    gdf = gpd.read_file(file_path)

    # Ensure a CRS exists and reproject to EPSG:4326 if necessary
    if gdf.crs is None:
        raise ValueError("The provided spatial file has no defined CRS.")
        
    if gdf.crs.to_epsg() != 4326:
        original_crs_name = gdf.crs.name
        gdf = gdf.to_crs(epsg=4326)
        print(f"Reprojecting ROI from {original_crs_name} to {gdf.crs.name}...")

    # Extract the bounding box of all features
    bounds = gdf.total_bounds  # Returns [minx, miny, maxx, maxy]
    bbox_geom = box(bounds[0], bounds[1], bounds[2], bounds[3])

    return bbox_geom.wkt

# ====================================================================================================   

def get_keycloak(username: str, password: str) -> str:
    """
    Retrieve an access token from the Copernicus Data Space Ecosystem (CDSE) 
    Keycloak identity service.
    
    This function performs an authentication request against the CDSE Keycloak 
    endpoint using the Resource Owner Password Credentials (ROPC) grant type. 
    It submits the provided username and password along with the public client 
    identifier to obtain a bearer access token, which can then be used to 
    authenticate subsequent API requests to Copernicus services.
    
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
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
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

def query_copernicus_catalog(
    data_collection,
    ROI,
    time_begin,
    time_end,
    cloud_max,
    cert_path,
    time_out
):
    """
    Query the Copernicus Data Space catalogue for satellite products 
    matching spatial, temporal, and quality constraints.

    This function constructs and executes an OData API request to the
    Copernicus Data Space Ecosystem catalogue. It filters products by
    collection name (e.g., Sentinel-2), spatial intersection with a
    Region of Interest (ROI), acquisition date range, and maximum cloud
    cover. The result is returned as a JSON response for downstream
    processing (e.g., metadata parsing, filtering, or download workflows).

    Args:
        data_collection (str): The name of the data collection to query
            (e.g., "SENTINEL-2", "SENTINEL-1").
        ROI (str): Region of Interest expressed as a WKT geometry string
            in EPSG:4326 (longitude/latitude).
        time_begin (str): Start date in ISO format (YYYY-MM-DD).
        time_end (str): End date in ISO format (YYYY-MM-DD).
        cloud_max (float): Maximum allowable cloud cover percentage (0–100).
        cert_path (str or bool): Path to SSL certificate bundle for secure
            requests, or True/None to use default verification.
        time_out (int or float): Timeout duration (seconds) for the HTTP request.

    Returns:
        dict: Parsed JSON response containing matching products and metadata.
        Returns None if the request fails.

    Raises:
        None: All network and request-related exceptions are caught internally
        to prevent interruption of automated workflows.

    Notes:
        - Uses the Copernicus OData endpoint with spatial filtering via
          ``OData.CSC.Intersects``.
        - Limits results to a maximum of 1000 products per request.
        - Cloud filtering is applied via the "cloudCover" attribute.
        - URL encoding (spaces and special characters) is handled prior
          to submission.
        - Designed for integration into bulk querying and download pipelines
          where resilience and fault tolerance are critical.
    """
    print("Starting query to Copernicus Data Space...")
    try:
        url = (
            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
            f"$filter=Collection/Name eq '{data_collection}' and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{ROI}') and "
            f"ContentDate/Start gt {time_begin}T00:00:00.000Z and "
            f"ContentDate/Start lt {time_end}T00:00:00.000Z and "
            f"Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {float(cloud_max)})"
            f"&$count=True&$top=1000"
        )

        url = html.unescape(url).replace(" ", "%20")
        print(f"Query URL:\n{url}")

        response = requests.get(url, verify=cert_path if cert_path else True, timeout=time_out)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# ====================================================================================================

def parse_footprints(json_data):
    """
    Parse Copernicus catalogue JSON response into a GeoDataFrame of product footprints.

    This function extracts product metadata from the Copernicus Data Space
    API response and converts the embedded GeoFootprint information into
    valid Shapely geometries. The resulting dataset is returned as a
    GeoPandas GeoDataFrame in geographic coordinates (EPSG:4326), ready
    for spatial filtering, visualisation, or downstream processing.

    Args:
        json_data (dict): JSON response returned from the Copernicus catalogue
            query, expected to contain a "value" key with a list of products.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing product metadata and
        associated geometries in EPSG:4326. Returns None if no valid products
        are found or geometry parsing fails.

    Raises:
        None: All parsing and conversion errors are handled internally to
        ensure robustness in batch workflows.

    Notes:
        - Geometries are parsed via ``_to_geom`` to handle WKT strings,
          GeoJSON-like dictionaries, and Copernicus GeoFootprint formats.
        - A new column ``identifier`` is derived from the product "Name"
          (substring before the first period) for easier referencing.
        - Prints all available product names for quick inspection and debugging.
        - Assumes input geometries are provided in EPSG:4326 (WGS84).
        - Invalid or missing geometries will result in None values, which
          can be filtered downstream if required.
    """
    if "value" not in json_data or not json_data["value"]:
        print("No products found in the response.")
        return None

    p = pd.DataFrame.from_dict(json_data["value"])
    print(f"Raw product count: {p.shape[0]}")

    try:
        p["geometry"] = p["GeoFootprint"].apply(_to_geom)
    except Exception as e:
        print(f"Error parsing GeoFootprint geometries: {e}")
        return None

    productDF = gpd.GeoDataFrame(p, geometry="geometry", crs="EPSG:4326")
    productDF["identifier"] = productDF["Name"].str.split(".").str[0]

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

def apply_overlap_reduction(tiles_to_download, ROI, min_roi_coverage_frac):
    """
    Reduce overlapping satellite tiles by retaining only those that sufficiently cover a Region of Interest (ROI).

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

    if len(tiles_used) > 0:
        print("\nPer-tile ROI coverage (before thresholding):")
        for _, r in tiles_used.sort_values("__frac", ascending=False).iterrows():
            print(f"  {r['Name']} -> {r['__frac']*100:.2f}% of ROI")

    # Retain best tile per acquisition above threshold
    kept_rows = []
    dropped_acqs = 0
    for acq, g in tiles_used.groupby("__acq_group", dropna=False):
        g_ok = g[g["__frac"] >= float(min_roi_coverage_frac)].copy()
        if g_ok.empty:
            dropped_acqs += 1
            continue
        best = g_ok.sort_values(by=["__frac", "__inter_m2", "Name"], ascending=[False, False, True]).head(1)
        kept_rows.append(best)

    kept = pd.concat(kept_rows, ignore_index=True) if kept_rows else tiles_used.iloc[0:0].copy()

    if dropped_acqs > 0:
        print(f"Acquisitions dropped by min_roi_coverage_frac={min_roi_coverage_frac}: {dropped_acqs}")

    if not kept.empty:
        print(f"\nTiles meeting min_roi_coverage_frac >= {min_roi_coverage_frac:.2f}:")
        for _, row in kept.iterrows():
            print(f"  {row['Name']} -> ROI coverage: {row['__frac']*100:.2f}%")

    return kept.drop(columns=["__deg_intersect_area", "__acq_group", "__inter_m2", "__frac", "scene_key_tile"], errors="ignore").copy()

# ====================================================================================================

def execute_downloads(
    tiles_to_download,
    save_folder,
    copernicus_user,
    copernicus_password,
    max_parallel_downloads,
    cert_path,
    time_out
):
    """
    Execute authenticated downloads of Copernicus products (sequentially or in parallel).

    This function manages end-to-end downloading of satellite products from the
    Copernicus Data Space Ecosystem. It handles authentication via Keycloak,
    redirects to the storage endpoint, streamed file downloads, token refresh,
    and error handling. Downloads can be performed either sequentially or using
    multithreading for improved performance.

    Args:
        tiles_to_download (gpd.GeoDataFrame): GeoDataFrame containing products
            to download. Must include "Name", "Id", and "identifier" fields.
        save_folder (str): Local directory path where downloaded ZIP files
            will be saved.
        copernicus_user (str): Username for Copernicus Data Space authentication.
        copernicus_password (str): Password for Copernicus Data Space authentication.
        max_parallel_downloads (int): Number of parallel download workers.
            Use 1 for sequential execution.
        cert_path (str or bool): Path to SSL certificate bundle for secure
            requests, or True/None to use system defaults.
        time_out (int or float): Timeout duration (seconds) for HTTP requests.

    Returns:
        tuple:
            - list[str]: Product IDs successfully downloaded.
            - list[str]: Product names that failed or were skipped.

    Raises:
        None: All network, authentication, and I/O errors are handled internally
        to ensure robustness in automated download pipelines.

    Notes:
        - Uses Keycloak token-based authentication with periodic refresh to
          prevent session expiry during long download batches.
        - Handles HTTP redirects to final storage endpoints before streaming data.
        - Files are downloaded as ZIP archives and saved using the product
          "identifier" as the filename.
        - Validates response content type to ensure only ZIP files are written.
        - Provides progress feedback including elapsed time during downloads.
        - Parallel mode uses ``ThreadPoolExecutor`` with thread-safe tracking
          of successful and failed downloads via a lock.
        - Designed for reliability under unstable network conditions and large
          batch processing scenarios.
    """
    downloaded_files = []
    failed_downloads = []
    
    if max_parallel_downloads <= 1:
        with requests.Session() as session:
            try:
                print("Authenticating with Copernicus Keycloak...")
                keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                print("Token retrieved successfully.")
                session.headers.update({"Authorization": f"Bearer {keycloak_token}"})

                for index, feat in tqdm(tiles_to_download.iterrows(), total=len(tiles_to_download)):
                    if index % 20 == 0 and index != 0:
                        print("Refreshing token...")
                        keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                        session.headers.update({"Authorization": f"Bearer {keycloak_token}"})

                    name, product_id, identifier = feat["Name"], feat["Id"], feat["identifier"]
                    print(f"\nStarting download for: {name}")
                    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"

                    try:
                        start_time = time.time()
                        response = session.get(url, allow_redirects=False, verify=cert_path if cert_path else True, timeout=time_out)

                        if response.status_code in (301, 302, 303, 307) and "Location" in response.headers:
                            redirect_url = response.headers["Location"]
                            print(f"Redirecting to: {redirect_url}")

                            def attempt_download():
                                return session.get(redirect_url, verify=cert_path if cert_path else True, timeout=time_out, stream=True)

                            file = attempt_download()

                            if file.status_code == 401:
                                print("Retrying after refreshing token...")
                                keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                                session.headers.update({"Authorization": f"Bearer {keycloak_token}"})
                                file = attempt_download()

                            file_path = os.path.join(save_folder, f"{identifier}.zip")
                            content_type = file.headers.get("Content-Type", "")
                            
                            if "application/zip" in content_type and file.status_code == 200:
                                with open(file_path, "wb") as f:
                                    last_tick = time.time()
                                    for chunk in file.iter_content(chunk_size=8192):
                                        if chunk:
                                            f.write(chunk)
                                            now = time.time()
                                            if now - last_tick >= 5:
                                                elapsed = time.time() - start_time
                                                elapsed_str = time.strftime("%H:%M:%S", time.gmtime(int(elapsed)))
                                                sys.stdout.write(f"\rDownloading {name}... [{elapsed_str}]")
                                                sys.stdout.flush()
                                                last_tick = now

                                elapsed = time.time() - start_time
                                mins, secs = divmod(elapsed, 60)
                                hours, mins = divmod(mins, 60)
                                elapsed_str = f"{int(hours):02}:{int(mins):02}:{int(secs):02}.{int((secs - int(secs)) * 1000):03}"
                                sys.stdout.write(f"\rDownloaded: {name} in {elapsed_str}\n")
                                sys.stdout.flush()
                                downloaded_files.append(product_id)
                            else:
                                print(f"\x1b[93mSkipped {name}: Unexpected content type '{content_type}' or status code {file.status_code}\x1b[0m")
                                try:
                                    print("Error response:", file.json())
                                except Exception:
                                    pass
                                failed_downloads.append(name)
                        else:
                            print(f"\x1b[93mNo redirect received for {name}, status code: {response.status_code}\x1b[0m")
                            failed_downloads.append(name)

                    except Exception as e:
                        elapsed = time.time() - start_time
                        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(int(elapsed)))
                        print(f"\x1b[91mError downloading {name}: {e}\x1b[0m")
                        print(f"Elapsed time: {elapsed_str}")
                        failed_downloads.append(name)

            except Exception as e:
                print(f"\x1b[91mAuthentication or session error: {e}\x1b[0m")
    else:
        lock = Lock()

        def _download_single(feat_row):
            name, product_id, identifier = feat_row["Name"], feat_row["Id"], feat_row["identifier"]
            print(f"\nStarting download for: {name}")

            with requests.Session() as session:
                try:
                    keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                    session.headers.update({"Authorization": f"Bearer {keycloak_token}"})

                    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
                    start_time = time.time()
                    response = session.get(url, allow_redirects=False, verify=cert_path if cert_path else True, timeout=time_out)

                    if response.status_code in (301, 302, 303, 307) and "Location" in response.headers:
                        redirect_url = response.headers["Location"]
                        print(f"Redirecting to: {redirect_url}")

                        def attempt_download():
                            return session.get(redirect_url, verify=cert_path if cert_path else True, timeout=time_out, stream=True)

                        file = attempt_download()

                        if file.status_code == 401:
                            print("Retrying after refreshing token...")
                            keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                            session.headers.update({"Authorization": f"Bearer {keycloak_token}"})
                            file = attempt_download()

                        file_path = os.path.join(save_folder, f"{identifier}.zip")
                        content_type = file.headers.get("Content-Type", "")
                        
                        if "application/zip" in content_type and file.status_code == 200:
                            with open(file_path, "wb") as f:
                                last_tick = time.time()
                                for chunk in file.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                                        now = time.time()
                                        if now - last_tick >= 5:
                                            elapsed = time.time() - start_time
                                            elapsed_str = time.strftime("%H:%M:%S", time.gmtime(int(elapsed)))
                                            sys.stdout.write(f"\rDownloading {name}... [{elapsed_str}]")
                                            sys.stdout.flush()
                                            last_tick = now

                            elapsed = time.time() - start_time
                            mins, secs = divmod(elapsed, 60)
                            hours, mins = divmod(mins, 60)
                            elapsed_str = f"{int(hours):02}:{int(mins):02}:{int(secs):02}.{int((secs - int(secs)) * 1000):03}"
                            sys.stdout.write(f"\rDownloaded: {name} in {elapsed_str}\n")
                            sys.stdout.flush()
                            with lock:
                                downloaded_files.append(product_id)
                        else:
                            print(f"\x1b[93mSkipped {name}: Unexpected content type '{content_type}' or status code {file.status_code}\x1b[0m")
                            with lock:
                                failed_downloads.append(name)
                    else:
                        print(f"\x1b[93mNo redirect received for {name}, status code: {response.status_code}\x1b[0m")
                        with lock:
                            failed_downloads.append(name)

                except Exception as e:
                    elapsed = time.time() - start_time if 'start_time' in locals() else 0
                    elapsed_str = time.strftime("%H:%M:%S", time.gmtime(int(elapsed)))
                    print(f"\x1b[91mError downloading {name}: {e}\x1b[0m")
                    print(f"Elapsed time: {elapsed_str}")
                    with lock:
                        failed_downloads.append(name)

        records = list(tiles_to_download.to_dict("records"))
        max_workers = int(max(1, max_parallel_downloads))
        print(f"\nParallel downloads enabled: {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_download_single, rec) for rec in records]
            for _ in as_completed(futures):
                pass 

    return downloaded_files, failed_downloads

# ====================================================================================================

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
    cloud_max=100
):
    """
    End-to-end pipeline for querying, filtering, and downloading Copernicus satellite data.

    This function orchestrates the full workflow for retrieving satellite imagery
    from the Copernicus Data Space Ecosystem. It integrates catalogue querying,
    footprint parsing, product-type filtering, optional baseline selection,
    spatial overlap reduction, and authenticated file downloads (sequential or parallel).
    The goal is to produce a robust, automated pipeline for acquiring only the most
    relevant and spatially valid datasets for a given Region of Interest (ROI).

    Args:
        data_collection (str): Name of the Copernicus data collection
            (e.g., "SENTINEL-2", "SENTINEL-1").
        ROI (str): Region of Interest as a WKT geometry string in EPSG:4326.
        time_begin (str): Start date in ISO format (YYYY-MM-DD).
        time_end (str): End date in ISO format (YYYY-MM-DD).
        copernicus_user (str): Username for Copernicus Data Space authentication.
        copernicus_password (str): Password for Copernicus Data Space authentication.
        product_type (list[str] or None): Product type filters (e.g., ['L1C', 'L2A']).
        save_folder (str): Directory path for storing downloaded files and logs.
        time_out (int or float, optional): HTTP timeout in seconds. Defaults to 2400.
        download_latest_only (bool, optional): If True, keeps only the most recent
            processing baseline per acquisition. Defaults to False.
        enable_overlap_reduction (bool, optional): If True, filters tiles based on
            ROI coverage thresholds. Defaults to True.
        cert_path (str or None, optional): Path to SSL certificate bundle. If None,
            uses ``certifi`` (if available). Defaults to None.
        max_parallel_downloads (int, optional): Number of parallel download workers.
            Use 1 for sequential execution. Defaults to 1.
        min_roi_coverage_frac (float, optional): Minimum fraction (0–1) of ROI
            coverage required for a tile to be retained. Defaults to 0.75.
        cloud_max (float, optional): Maximum allowable cloud cover percentage (0–100).
            Defaults to 100.

    Returns:
        list: List of successfully downloaded product IDs. Returns an empty list
        if no downloads were completed.

    Raises:
        None: All errors in querying, parsing, filtering, and downloading are
        handled internally to ensure pipeline resilience.

    Notes:
        - Automatically configures SSL certificates using ``certifi`` if not provided.
        - Implements a staged filtering pipeline:
            1. Catalogue query (spatial + temporal + cloud filtering)
            2. Geometry parsing into GeoDataFrame
            3. Product-type filtering (e.g., L1C/L2A)
            4. Optional baseline reduction (latest processing version)
            5. Optional spatial overlap reduction (ROI coverage threshold)
        - Ensures only tiles with sufficient spatial relevance are downloaded,
          reducing redundancy and storage overhead.
        - Supports both sequential and multithreaded downloads for scalability.
        - Generates a CSV log of successfully downloaded product IDs for auditing.
        - Provides verbose console output for traceability and debugging.
        - Designed for integration into automated EO data pipelines and batch workflows.
    """
    # Certificate configuration
    if cert_path is None and _certifi is not None:
        cert_path = _certifi.where()

    # Configure output directory
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
        print(f"Created folder: {save_folder}")

    # 1. Fetch JSON manifest
    json_data = query_copernicus_catalog(
        data_collection, ROI, time_begin, time_end, cloud_max, cert_path, time_out
    )
    if not json_data:
        return

    # 2. Extract Footprints into GeoDataframe
    productDF = parse_footprints(json_data)
    if productDF is None or productDF.empty:
        return

    # 3. Filter by product type flag ('L1C', 'L2A', etc.)
    filtered_productDF = filter_by_product_type(productDF, product_type)
    if filtered_productDF.empty:
        print("No tiles matched the specified product type.")
        return

    # 4. Optional "latest baseline" filter
    if download_latest_only:
        tiles_to_download = keep_latest_baseline_only(filtered_productDF)
    else:
        tiles_to_download = filtered_productDF.copy()

    # 5. Execute overlap reduction thresholds
    if enable_overlap_reduction:
        tiles_to_download = apply_overlap_reduction(tiles_to_download, ROI, min_roi_coverage_frac)
    else:
        print("Skipping overlap reduction based on user preference.")

    # 6. Verify and Log Pre-download targets
    count_to_dl = 0 if tiles_to_download.empty else len(tiles_to_download)
    print(f"\nDownloading {'latest' if download_latest_only else 'all'} filtered tiles (post-coverage filter): {count_to_dl}")
    
    if count_to_dl > 0:
        for name in tiles_to_download["Name"]:
            print("\x1b[92m" + name + "\x1b[0m")
    else:
        print("No tiles meet the minimum ROI coverage threshold; nothing to download.")
        return []

    # 7. Execute sequence or threaded downloads
    downloaded_files, failed_downloads = execute_downloads(
        tiles_to_download, save_folder, copernicus_user, copernicus_password, max_parallel_downloads, cert_path, time_out
    )

    # 8. Post-Download Auditing
    t1 = time_begin.replace("-", "_")
    t2 = time_end.replace("-", "_")
    log_file_name = os.path.join(save_folder, f"log_{t1}_to_{t2}.csv")

    try:
        with open(log_file_name, "w") as log_file:
            for file_id in downloaded_files:
                log_file.write(f"{file_id}\n")
    except Exception as e:
        print(f"Warning: failed to write log file '{log_file_name}': {e}")

    if not downloaded_files:
        print('\x1b[93mNo data was successfully downloaded.\x1b[0m')

    if failed_downloads:
        print("\n\x1b[91mFailed downloads:\x1b[0m")
        for name in failed_downloads:
            print(name)

    return downloaded_files
    
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