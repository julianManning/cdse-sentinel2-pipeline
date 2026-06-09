# 🧑‍💻 cdse-sentinel2-pipeline

A high-throughput, multithreaded Python pipeline for querying and downloading Sentinel-2 imagery from the modern Copernicus Data Space Ecosystem (CDSE). It features dynamic ROI overlap reduction to discard redundant edge-tiles, a fully automated Conda/Jupyter environment setup, and batch extraction to analysis-ready `.SAFE` directories.

---

## 🛰️ The Problem: A Broken Ecosystem 

If you work with satellite imagery, you likely know the bad news: the legacy **Copernicus Open Access Hub (SciHub) has been officially retired.** Almost overnight, hundreds of automated Python scripts and GIS pipelines broke, leaving developers scrambling to adapt to the new Copernicus Data Space Ecosystem (CDSE).

While the standard CDSE web portal is fine for downloading a few single scenes, teams tackling **complex study areas, large geographic boundaries, or extensive time-series datasets** need an automated, programmatic pipeline.

### Why not just use Cloud platforms (GEE, Planetary Computer)?
While cloud-native ecosystems have made basic Earth Observation analysis accessible, scaling professional pipelines on public cloud infrastructure routinely hits three brick walls:
1. **Incomplete Processing Levels:** Many cloud repositories only host specific downstream products (e.g., Microsoft Planetary Computer exclusively hosts L2A). If your workflow requires raw Level-1C (Top-of-Atmosphere) imagery, you are locked out.
2. **The Commercial Paywall:** Tools like Google Earth Engine (GEE) are phenomenal for non-commercial research, but moving those identical workflows into an enterprise environment requires paid Google Cloud accounts with strict billing models tied to compute and storage consumption.
3. **Severe Quotas:** Processing extensive spatial boundaries on free cloud tiers routinely triggers server-side memory timeouts, strict API rate limits, and massive export queuing delays.

For uninhibited access to raw, native satellite archives without the overhead or restrictions, a highly optimized local downloader is a requirement.

---

## ⚡ Core Features

Instead of building a convoluted "black-box" Python package, this repository is designed as a highly transparent **Clone-and-Run Toolkit** utilizing a sequential Jupyter Notebook workflow. 

* **CDSE Native & OData Powered**: Built from scratch for the new infrastructure using modern OData catalogue querying and Keycloak OAuth2 authentication.
* **Smart Spatial Thresholding (Anti-Redundancy Filter)**: Standard scripts blindly download massive 1GB+ tiles if they touch your Region of Interest (ROI) by even 1%. This pipeline dynamically reprojects your spatial boundary to a local UTM coordinate system, computes the true geometric intersection area, and **drops redundant edge-tiles** falling below your custom coverage threshold (e.g., `< 70%`). Save massive amounts of disk space and hours of bandwidth.
* **High-Throughput Parallel Execution**: Accelerates heavy data streaming using a multi-worker `ThreadPoolExecutor` for concurrent downloads, fully saturating your network connection.
* **Turnkey Deployment (Zero Environment Friction)**: Avoid the classic "GDAL installation nightmare." The pipeline includes an automated, asynchronous environment builder that handles version-pinned Conda synchronization and registers a system-level Jupyter kernel automatically.
* **Automated Batch Extraction**: Automatically unpacks downloaded `.zip` archives into the strict `.SAFE` directory layouts mandated by native geospatial workflows.

---

## 📂 Repository Structure

```text
cdse-sentinel2-pipeline/
│
├── p00_s2_download_env_setup.ipynb   # Step 0: Turnkey workspace & kernel initialization
├── p01_s2_download_pipeline.ipynb    # Step 1: Spatial query, overlap filtering, & threaded download
├── p02_s2_download_unzip.ipynb       # Step 2: Automated extraction to structured .SAFE directories
│
├── DATA/                             # Local data vault (ignored by git bouncer)
│   └── UNZIP/                        # Target directory for unzipped .SAFE archives
│
├── Tools/                            # Core pipeline backend orchestration engines
│   ├── env.base.yml                  # Loose environment specs for development
│   ├── env.lock.yml                  # Fully version-pinned production lockfile
│   ├── s2_download_env_builder.py    # Conda & Jupyter lifecycle automation script
│   └── s2_download_helper_utility.py # Spatial parsing, API calling, and multithreading engines
│
└── Example/                          # Sample geospatial inputs for out-of-the-box evaluation
    └── SHP/                          # Target Region of Interest (ROI) vector layers
```

## 🚀 Execution Workflow

### Step 0: Workspace Initialization
Open and execute `p00_s2_download_env_setup.ipynb`. The automated runtime manager will:

* Scan for the dedicated `sentinel2` Conda environment.

* Synchronize dependency deltas securely from the version lockfile.

* Link the backend kernel path and register it cleanly into your Jupyter notebook system.

### Step 1: Data Acquisition & Overlap Filtering
Open `p01_s2_download_pipeline.ipynb` and select your newly minted `Python 3 (Sentinel2)` kernel.

* Input your study area vector dataset path.

* Define temporal ranges and max cloud-cover constraints.

* Configure `min_roi_coverage_frac` (e.g., `0.70` to strictly guarantee that tiles must cover at least 70% of your target ROI boundary).

* Run the pipeline to seamlessly authenticate with CDSE Keycloak and stream parallelized downloads.

### Step 2: Extraction (unzip)
Open `p02_s2_download_unzip.ipynb`. Run the automated compression cleaner to expand downloaded archives into the standard layout rules mandated by native `.SAFE` geospatial pipelines.

---

## 📋 Requirements
* Active user credentials on the Copernicus Data Space Ecosystem.

* An existing installation of Miniconda / Anaconda.
