# cdse-sentinel2-pipeline

A high-throughput Python pipeline for querying and downloading Sentinel-2 imagery from the modern Copernicus Data Space Ecosystem (CDSE). It features multithreaded execution, dynamic ROI overlap reduction to discard redundant edge-tiles, fully automated Conda/Jupyter environment setup, and batch extraction to analysis-ready .SAFE directories.

---

## 🛰️ Why Another Sentinel-2 Downloader?

If you search GitHub, you will find hundreds of satellite download scripts. **Most of them are currently broken.** ESA recently retired the legacy Copernicus Open Access Hub (SciHub), making tools built on old APIs completely obsolete. 

This repository begs to differ by offering a modern, stable, and incredibly resource-efficient solution to spatial data acquisition:

* **CDSE Native & OData Powered**: Built from scratch for the new Copernicus Data Space Ecosystem infrastructure using modern OData catalogue querying and Keycloak OAuth2 authentication.
* **Smart ROI Thresholding (Anti-Redundancy Filter)**: Standard scripts blindly download massive 1GB+ tiles if they touch your Region of Interest (ROI) by even 1%. This pipeline dynamically reprojects your spatial boundary to a local UTM coordinate system, computes the true geometric intersection area, and drops redundant edge-tiles falling below your custom coverage threshold. Save massive amounts of disk space and hours of bandwidth.
* **Turnkey Deployment (Zero Environment Friction)**: Avoid the classic "GDAL installation nightmare." The pipeline includes an automated, asynchronous environment builder that handles version-pinned Conda synchronization and registers a system-level Jupyter kernel automatically.
* **High-Throughput Parallel Execution**: Accelerates heavy data streaming using a multi-worker `ThreadPoolExecutor` for concurrent downloads, followed by automated batch extraction into structured GIS pipelines.

---

## 📂 Repository Structure

```text
cdse-sentinel2-pipeline/
│
├── env.base.yml                      # Loose environment specs for development
├── env.lock.yml                      # Fully version-pinned production lockfile
├── p00_s2_download_env_setup.ipynb   # Step 0: Turnkey workspace & kernel initialization
├── p01_s2_download_pipeline.ipynb    # Step 1: Spatial query, overlap filtering, & threaded download
├── p02_s2_download_unzip.ipynb       # Step 2: Automated extraction to structured .SAFE directories
│
├── DATA/                             # Local data vault (ignored by git bouncer)
│   └── UNZIP/                        # Target directory for unzipped .SAFE archives
│
├── Tools/                            # Core pipeline backend orchestration engines
│   ├── s2_download_env_builder.py    # Conda & Jupyter lifecycle automation script
│   └── s2_download_helper_utility.py # Spatial parsing, API calling, and multithreading engines
│
└── Example/                          # Sample geospatial inputs for out-of-the-box evaluation
    └── SHP/                          # Target Region of Interest (ROI) vector layers
