# EGMStoolkit — Technical Specification

Complete workflow and data specification for reimplementing EGMStoolkit in another language.

---

## Table of Contents

1. [Overview](#overview)
2. [CLI Options](#cli-options)
3. [Full Workflow](#full-workflow)
4. [Data Structures](#data-structures)
5. [API Endpoints & URL Patterns](#api-endpoints--url-patterns)
6. [File Naming Conventions](#file-naming-conventions)
7. [Output Directory Structure](#output-directory-structure)
8. [L3 Grid System](#l3-grid-system)
9. [Burst ID Map](#burst-id-map)
10. [ROI Detection Logic](#roi-detection-logic)
11. [Download Engine](#download-engine)
12. [Post-Processing](#post-processing)
13. [Error Handling & Retry Logic](#error-handling--retry-logic)
14. [Token & Authentication](#token--authentication)
15. [Constants](#constants)
16. [External Dependencies](#external-dependencies)

---

## Overview

EGMStoolkit downloads and post-processes InSAR displacement data from the European Ground Motion Service (EGMS) Copernicus programme. Given a region of interest (bbox, shapefile, or country code), it:

1. Identifies which Sentinel-1 burst IDs / L3 tiles overlap the ROI
2. Downloads the corresponding ZIP files from the EGMS API
3. Optionally unzips, merges, clips, and grids the data

**Coordinate systems used:**
- User input / output maps: EPSG:4326 (WGS84 geographic)
- Internal processing / EGMS data: EPSG:3035 (ETRS89-LAEA Europe, meters)

**Data levels:**
| Level | Content | Format | Unit |
|-------|---------|--------|------|
| L2a | Ascending InSAR time-series, per burst | CSV | mm/yr + mm per date |
| L2b | Descending InSAR time-series, per burst | CSV | mm/yr + mm per date |
| L3UD | Vertical velocity, 100km tiles | GeoTIFF | mm/yr |
| L3EW | East-West velocity, 100km tiles | GeoTIFF | mm/yr |

---

## CLI Options

Entry point: `EGMStoolkit` (registered console script)

| Flag | Dest | Type | Default | Description |
|------|------|------|---------|-------------|
| `-l, --level` | `level` | str | `L2a,L2b` | Data levels to download. Comma-separated subset of: `L2a`, `L2b`, `L3UD`, `L3EW` |
| `-r, --release` | `release` | str | `2019_2023` | EGMS release(s). Comma-separated subset of: `2015_2021`, `2018_2022`, `2019_2023` |
| `-t, --token` | `token` | str | `XXXXXXXXX` | EGMS user token(s). Comma-separated for multiple tokens (one per download worker) |
| `-b, --bbox` | `bbox` | str | `None` | Region of interest. Three formats accepted (see ROI Detection) |
| `-o, --outputdir` | `outputdir` | str | `Output` | Output directory path |
| `--track` | `track` | str | `None` | Relative orbit number(s) to filter L2, comma-separated |
| `--pass` | `passS1` | str | `None` | Orbit pass(es) to filter L2: `Ascending` or `Descending`, comma-separated. Must be same length as `--track` |
| `--nodownload` | `download` | bool | `True` | Pass flag to skip downloading |
| `--nounzip` | `unzip` | bool | `True` | Pass flag to skip unzipping |
| `--unzipworker` | `unzipworker` | int | `1` | Parallel workers for unzipping (uses joblib) |
| `--downloadworker` | `downloadworker` | int | `1` | Parallel workers for downloading (1–8) |
| `--nozip` | `nokeepzip` | bool | `True` | Pass flag to delete ZIP files after extraction |
| `--nomerging` | `merging` | bool | `True` | Pass flag to skip CSV/TIFF merging |
| `--noclipping` | `clipping` | bool | `True` | Pass flag to skip clipping/cropping to ROI |
| `--clean` | `clean` | bool | `False` | Pass flag to remove raw data directories after processing |
| `-q, --quiet` | `verbose` | bool | `True` | Pass flag to suppress console output |
| `--nolog` | `logmode` | bool | `True` | Pass flag to disable log file (`egmstoolkit.log`) |
| `--docs` | `docmode` | bool | `False` | Open HTML documentation in browser |

**Validation rules:**
- Token must not be `XXXXXXXXX` (placeholder)
- `--track` and `--pass` must have identical comma-count if both provided
- bbox must not be `None`
- `--downloadworker` must be integer in range [1, 8]

---

## Full Workflow

```
CLI args
   │
   ├─[1] Validate inputs
   │
   ├─[2] Download Burst ID Map (if not cached)
   │      └─ S1burstIDapi.S1burstIDmap.downloadfile()
   │
   ├─[3] For each bbox × level × release:
   │      ├─ Parse bbox → bbox.shp
   │      ├─ S1ROIapi.S1ROIparameter.createROI()
   │      ├─ S1ROIapi.S1ROIparameter.detectfromIDmap()
   │      ├─ egmsdownloader.updatelist()
   │      └─ S1ROIapi.S1ROIparameter.displaymap() → fig_search_N.jpg
   │
   ├─[4] Download files
   │      └─ egmsdownloader.download()
   │             ├─ Pre-create output directories
   │             ├─ Build work list (url, path, filename, token)
   │             └─ Parallel dispatch via joblib (threading backend)
   │
   ├─[5] Generate post-download map (if any missing/failed)
   │      └─ S1ROIapi.S1ROIparameter.displaymap() → fig_missing.jpg
   │
   ├─[6] Unzip files
   │      └─ egmsdownloader.unzipfile()
   │             └─ Parallel via joblib
   │
   ├─[7] Merge (optional)
   │      ├─ egmsdatatools.datamergingcsv()   → merged CSV / VRT
   │      └─ egmsdatatools.datamergingtiff()  → merged TIFF (L3 only)
   │
   ├─[8] Clip (optional)
   │      └─ egmsdatatools.dataclipping()
   │             ├─ CSV: point-in-polygon filter
   │             └─ TIFF: rio mask --crop
   │
   └─[9] Clean raw data (optional)
          └─ egmsdatatools.removerawdata()
```

### Step-by-step detail

#### [1] Validate inputs
- Token != `XXXXXXXXX`
- bbox != `None`
- Create `outputdir` if missing
- Set log file to `egmstoolkit.log` (or `None` if `--nolog`)

#### [2] Burst ID Map
- See [Burst ID Map](#burst-id-map) section

#### [3] ROI detection loop

```
for each bbox in bbox_list:
    ROIpara = S1ROIparameter(bbox, level, ...)
    ROIpara.createROI()           # bbox → bbox.shp
    
    if level in [L2a, L2b]:
        ROIpara.detectfromIDmap(burstIDmap, track_filter, pass_filter)
    elif level == L3:
        ROIpara.detectfromIDmap(burstIDmap)   # L3 tiles generated internally
    
    for release in releases:
        ROIpara.release = release
        downloader.updatelist(ROIpara)        # appends to download lists
    
    ROIpara.displaymap(output=fig_search_N.jpg)
```

**Multiple levels:** If `-l L2a,L2b,L3UD`, the loop runs once per level per bbox, creating a separate `S1ROIparameter` instance each time.

**Multiple releases:** `updatelist()` is called once per release inside the inner loop; download lists are deduplicated via `numpy.unique()`.

#### [4] Download
- See [Download Engine](#download-engine) section

#### [5] Post-download map
- Called only if `downloader.missing` or `downloader.failed` or `downloader.downloaded` is non-empty
- Draws green polygons for downloaded tiles, grey polygons for missing/failed
- Only L3 tiles decoded (filename contains `E{col}N{row}` pattern)
- Saved as `{outputdir}/fig_missing.jpg`

#### [6] Unzip
```
glob pattern: {outputdir}/*/*/*/*.zip
for each zip:
    extract to: {zip_dir}/{zip_stem}/
    if cleanmode: delete zip
```

#### [7] Merge

**CSV merge** (`datamergingcsv`):
- Groups files by: level + track number
- Detects headers and time-series columns (columns containing `'20'` in name)
- Concatenates rows from all bursts in group
- Optional duplicate removal using concave hull
- Writes merged CSV to `outputdir/{level}_{track}.csv`

**TIFF merge** (`datamergingtiff`):
- Finds all `.tiff` files in output tree
- Calls `gdal_merge` — see [Post-Processing](#post-processing)

#### [8] Clip

**CSV clip:**
- Reads bbox.shp polygon in EPSG:3035
- Tests each point: `easting, northing` inside polygon
- Writes matching rows to `{name}_clipped.csv`

**TIFF crop:**
- Converts bbox.shp to GeoJSON in EPSG:3035
- Calls `rio mask --crop` — see [Post-Processing](#post-processing)

#### [9] Clean
- Removes directories: `{outputdir}/L2a/`, `L2b/`, `L3UD/`, `L3EW/`

---

## Data Structures

### S1ROIparameter

```
bbox          str | None     User input (WSEN string / country code / shapefile path)
ROIs          str            Path to generated bbox.shp
egmslevel     str            'L2a' | 'L2b' | 'L3'
egmsL3component str          'UD' | 'EW'  (L3 only)
release       str            '2015_2021' | '2018_2022' | '2019_2023'
Data          dict           L2 burst detection results (see below)
DataL3        dict           L3 tile detection results (see below)
workdirectory str            Directory where bbox.shp is written
verbose       bool
log           str | None
```

### Data (L2 burst results)

Key format: `'{orbit_pass}_{relative_orbit:04d}'`  e.g. `'Ascending_0124'`

```python
Data = {
    'Ascending_0124': {
        'IW1': [
            {
                'relative_orbit_number': int,   # 1–175
                'subswath_name': str,            # 'IW1' | 'IW2' | 'IW3'
                'orbit_pass': str,               # 'Ascending' | 'Descending'
                'esa_burst_id': int,
                'egms_burst_id': int,            # 4-digit EGMS ID
                'polyburst': Polygon             # shapely Polygon, EPSG:4326
            },
            ...
        ],
        'IW2': [...],
        'IW3': [...]
    },
    ...
}
```

### DataL3 (L3 tile results)

```python
DataL3 = {
    'Tileinfo': ['Tile L3', ...],       # one label per tile
    'polyL3':   [Polygon, ...],         # shapely Polygons in EPSG:3035
    'polyL3ll': [Polygon, ...]          # shapely Polygons in EPSG:4326
}
```

### egmsdownloader

```
listL2a       list[str]   L2a filenames
listL2alink   list[str]   L2a base URLs (without token)
listL2b       list[str]   L2b filenames
listL2blink   list[str]   L2b base URLs
listL3UD      list[str]   L3UD filenames
listL3UDlink  list[str]   L3UD base URLs
listL3EW      list[str]   L3EW filenames
listL3EWlink  list[str]   L3EW base URLs
tokens        list[str]   One or more user tokens
missing       list[str]   Filenames not available on server (502 exhausted)
failed        list[str]   Filenames that errored (network/HTTP error)
downloaded    list[str]   Successfully downloaded filenames
verbose       bool
log           str | None
```

### S1burstIDmap

```
date_str_init  str         '29/05/2022'  (earliest known release)
dirmap         str         Path to 3rdparty/ directory
pathIDmap      str | None  Full path to active .sqlite3 file
list_date      list[str]   Generated candidate dates (newest first)
verbose        bool
log            str | None
```

---

## API Endpoints & URL Patterns

### EGMS Download API

**Base URL:** `https://egms.land.copernicus.eu/insar-api/archive/download/`

**Full download URL:**
```
{base_url}{filename}?id={token}
```

**L2a/L2b filename:**
```
EGMS_{level}_{relative_orbit:03d}_{egms_burst_id:04d}_IW{subswath}_VV{release_suffix}.zip

Examples:
  EGMS_L2a_001_0280_IW3_VV.zip                    (release 2015_2021)
  EGMS_L2a_052_0787_IW2_VV_2018_2022_1.zip
  EGMS_L2b_161_0296_IW3_VV_2019_2023_1.zip
```

**L3 filename:**
```
EGMS_L3_E{col:2d}N{row:2d}_100km_{component}{release_suffix}.zip

Examples:
  EGMS_L3_E32N29_100km_U.zip                      (release 2015_2021, UD)
  EGMS_L3_E33N26_100km_U_2018_2022_1.zip
  EGMS_L3_E40N23_100km_E_2019_2023_1.zip          (EW)
```

**col/row derivation:**
```python
col = int(tile_polygon_3035.exterior.coords.xy[0][0] / 100000)   # SW corner X / 100km
row = int(tile_polygon_3035.exterior.coords.xy[1][0] / 100000)   # SW corner Y / 100km
```

**Release suffixes:**

| Release | Suffix |
|---------|--------|
| 2015_2021 | `` (empty string) |
| 2018_2022 | `_2018_2022_1` |
| 2019_2023 | `_2019_2023_1` |

**HTTP behaviour:**

| Status | Meaning | Action |
|--------|---------|--------|
| 200 | OK, full file | Stream to disk |
| 206 | Partial content (resume) | Append to disk |
| 416 | Range not satisfiable | File already complete, skip |
| 429 | Too many requests | Exponential backoff + retry |
| 502 | Bad gateway | Exponential backoff + retry (may mean file doesn't exist) |
| Other | Error | Return as failed |

**Resume support:** All requests include `Range: bytes={existing_size}-` header. If file partially exists, download resumes from that offset (append mode).

### Burst ID Map

```
https://sar-mpc.eu/files/S1_burstid_{YYYYMMDD}.zip

Example: https://sar-mpc.eu/files/S1_burstid_20250414.zip
```

Authentication: none required.

---

## File Naming Conventions

### Release suffix function

```python
def release_suffix(release):
    if release == '2015_2021': return ''
    if release == '2018_2022': return '_2018_2022_1'
    if release == '2019_2023': return '_2019_2023_1'
```

### L2a/L2b filename construction

```python
name = 'EGMS_{level}_{orbit:03d}_{burst_id:04d}_IW{iw}_VV{suffix}.zip'.format(
    level   = 'L2a' or 'L2b',
    orbit   = relative_orbit_number,
    burst_id = egms_burst_id,
    iw      = '1', '2', or '3',
    suffix  = release_suffix(release)
)
```

### L3 filename construction

```python
# Component: 'U' for UD, 'E' for EW
component = 'U' if egmsL3component == 'UD' else 'E'

x = tile_polygon_3035.exterior.coords.xy[0][0] / 100000   # SW X in 100km units
y = tile_polygon_3035.exterior.coords.xy[1][0] / 100000   # SW Y in 100km units

name = 'EGMS_L3_E{x:2d}N{y:2d}_100km_{component}{suffix}.zip'.format(
    x         = int(y),   # NOTE: x in filename = northing (row)
    y         = int(x),   # NOTE: y in filename = easting (col)
    component = component,
    suffix    = release_suffix(release)
)
```

> **Note on E/N axis naming:** Despite the letters, `E{a}` encodes the **northing** index and `N{b}` encodes the **easting** index. This is a legacy quirk of the EGMS dataset. To reconstruct a tile polygon: `x0_easting = b * 100000`, `y0_northing = a * 100000`.

### Reverse-parse filename → release

```python
def release_from_filename(name):
    stem = name.split('.')[0]                # drop .zip
    part = stem.split('VV')[-1]             # suffix after 'VV'
    if '_2018_2022_1' in part: return '2018_2022'
    if '_2019_2023_1' in part: return '2019_2023'
    return '2015_2021'
```

---

## Output Directory Structure

```
{outputdir}/
├── L2a/
│   ├── 2015_2021/
│   │   ├── EGMS_L2a_001_0280_IW3_VV.zip
│   │   └── EGMS_L2a_001_0280_IW3_VV/
│   │       └── EGMS_L2a_001_0280_IW3_VV.csv
│   ├── 2018_2022/
│   └── 2019_2023/
├── L2b/
│   ├── 2015_2021/
│   ├── 2018_2022/
│   └── 2019_2023/
├── L3UD/
│   ├── 2015_2021/
│   ├── 2018_2022/
│   └── 2019_2023/
│       ├── EGMS_L3_E32N29_100km_U_2019_2023_1.zip
│       └── EGMS_L3_E32N29_100km_U_2019_2023_1/
│           └── EGMS_L3_E32N29_100km_U_2019_2023_1.tiff
├── L3EW/
│   └── ...
├── {level}_{track}.csv          (merged L2, e.g. L2a_0124.csv)
├── {level}_{track}.vrt          (optional VRT index)
├── {level}_{track}_clipped.csv  (clipped to ROI)
├── L3UD_merged.tiff             (merged L3)
├── L3UD_merged_cropped.tiff     (clipped L3)
├── fig_search_0.jpg             (ROI detection map, before download)
├── fig_search_1.jpg             (additional bbox maps if multiple ROIs)
├── fig_missing.jpg              (post-download status map, green/grey tiles)
├── bbox.shp                     (user ROI as shapefile, EPSG:4326)
├── bbox.shx
├── bbox.dbf
├── bbox.prj
└── egmstoolkit.log              (if logmode enabled)
```

**Path construction for a downloaded file:**
```python
type_dir  = os.path.join(outputdir, type_label)           # e.g. outputdir/L2a
release_dir = os.path.join(type_dir, release)             # e.g. outputdir/L2a/2019_2023
output_file = os.path.join(release_dir, filename)         # e.g. outputdir/L2a/2019_2023/EGMS_...zip
```

**Unzip target:**
```python
unzip_dir = os.path.join(release_dir, filename_without_extension)
# e.g. outputdir/L2a/2019_2023/EGMS_L2a_001_0280_IW3_VV_2019_2023_1/
```

---

## L3 Grid System

L3 data is published on a 100km × 100km grid in EPSG:3035.

### Grid bounds

```python
X_MIN, X_MAX = 900000, 7400000    # EPSG:3035 easting range (meters)
Y_MIN, Y_MAX = 900000, 7400000    # EPSG:3035 northing range (meters)
TILE_SIZE    = 100000             # 100 km
BUFFER       = 5                  # extra tiles searched beyond bounds
```

### Tile generation algorithm

```python
import numpy as np
from pyproj import Transformer
from shapely.geometry import Polygon

meter_to_latlon = Transformer.from_crs('epsg:3035', 'epsg:4326', always_xy=False)

x_indices = range(int(X_MIN/TILE_SIZE) - BUFFER, int(X_MAX/TILE_SIZE) + BUFFER)
y_indices = range(int(Y_MIN/TILE_SIZE) - BUFFER, int(Y_MAX/TILE_SIZE) + BUFFER)

tiles = []
for xi in x_indices:
    for yi in y_indices:
        # EPSG:3035 corners (SW origin)
        xs = [xi*TILE_SIZE, (xi+1)*TILE_SIZE, (xi+1)*TILE_SIZE, xi*TILE_SIZE, xi*TILE_SIZE]
        ys = [yi*TILE_SIZE, yi*TILE_SIZE, (yi+1)*TILE_SIZE, (yi+1)*TILE_SIZE, yi*TILE_SIZE]
        poly_3035 = Polygon(zip(xs, ys))

        # Convert to EPSG:4326 for ROI intersection test
        lats, lons = meter_to_latlon.transform(xs, ys)
        poly_4326 = Polygon(zip(lons, lats))

        if roi_polygon.intersects(poly_4326):
            tiles.append({'poly_3035': poly_3035, 'poly_4326': poly_4326})
```

### Reverse-parse tile coordinates from filename

```python
import re
m = re.search(r'E(\d+)N(\d+)', filename)
col = int(m.group(1))   # maps to Y axis (northing / 100km)
row = int(m.group(2))   # maps to X axis (easting / 100km)

# EPSG:3035 SW corner of tile:
x0 = col * 100000
y0 = row * 100000
```

---

## Burst ID Map

### Location

Downloaded to: `{package_dir}/3rdparty/S1_burstid_{YYYYMMDD}/`

### Download algorithm

```python
from datetime import date, timedelta

start = date(2022, 5, 29)    # earliest known map
today = date.today()

# Generate dates newest-first
candidate_dates = []
d = today
while d >= start:
    candidate_dates.append(d.strftime('%Y%m%d'))
    d -= timedelta(days=1)

for date_str in candidate_dates:
    url = f'https://sar-mpc.eu/files/S1_burstid_{date_str}.zip'
    # Try HTTP HEAD or GET; if 200, download and stop
```

### ZIP contents

```
S1_burstid_YYYYMMDD.zip
└── IW/
    └── sqlite/
        └── S1_burstid_YYYYMMDD.sqlite3    ← main database
```

### SQLite schema (relevant columns)

| Column | Type | Description |
|--------|------|-------------|
| `relative_orbit_number` | int | Relative orbit (1–175) |
| `subswath_name` | str | `'IW1'` / `'IW2'` / `'IW3'` |
| `orbit_pass` | str | `'Ascending'` / `'Descending'` |
| `burst_id` | int | ESA burst ID |
| `time_from_anx_sec` | float | Time from ascending node crossing (seconds) |
| geometry | MultiPolygon | Burst footprint in EPSG:4326 |

Accessed via Fiona with driver `'SQLite'`.

### ESA burst ID → EGMS burst ID conversion

```python
AZ_SIZE = 750           # azimuth size
DT_AZ   = 0.0020555563 # azimuth timing (seconds)

def esa_to_egms_burst_id(relative_orbit_number, time_from_anx_sec):
    anx_mid = time_from_anx_sec + (AZ_SIZE / 2.0) * DT_AZ
    # Uses: esa2egmsburstID.get_egms_burst_cycle_id(relative_orbit_number, anx_mid)
    # Returns: 4-digit integer EGMS burst ID
```

---

## ROI Detection Logic

### Input format parsing

**Format 1: WSEN bounding box**
```
"-6.427059639290446,53.2606655698541,-6.0952332730202095,53.41811986118854"
Parsed as: [west, south, east, north] floats in EPSG:4326
```

**Format 2: Country code(s)**
```
"IE"  or  "IE,FR"
Requires: GMT (Generic Mapping Tools) installed
Command: gmt coast -E{code} -M  → extracts country polygon
Then: ogr2ogr to convert to EPSG:4326 shapefile
```

**Format 3: Shapefile path**
```
"/path/to/roi.shp"   or   "roi.shp"
Accepted geometry types: Polygon, MultiPolygon, LineString, MultiLineString
If CRS != EPSG:4326: reprojected automatically via pyproj
```

### bbox.shp creation

All input formats are normalised to a MultiLineString shapefile in EPSG:4326:

```
File: {workdirectory}/bbox.shp
Driver: ESRI Shapefile
CRS: EPSG:4326
Geometry type: MultiLineString
Schema: {'geometry': 'MultiLineString', 'properties': {}}
```

### L2a/L2b burst detection

```python
# 1. Read bbox.shp → list of Polygon objects in EPSG:4326

# 2. Transform ROI to EPSG:3035
latlon_to_meter = Transformer.from_crs('epsg:4326', 'epsg:3035')
roi_3035 = transform(latlon_to_meter.transform, roi_polygon)

# 3. Read burst SQLite with Fiona
with fiona.open(sqlite_path, driver='SQLite') as db:
    for feature in db:
        burst_geom = shape(feature['geometry'])     # EPSG:4326
        
        # 4. Intersection test in EPSG:4326
        if roi_polygon.intersects(burst_geom):
            burst = {
                'relative_orbit_number': feature['properties']['relative_orbit_number'],
                'subswath_name':         feature['properties']['subswath_name'],
                'orbit_pass':            feature['properties']['orbit_pass'],
                'esa_burst_id':          feature['properties']['burst_id'],
                'egms_burst_id':         esa_to_egms_burst_id(...),
                'polyburst':             burst_geom
            }
            key = f"{orbit_pass}_{relative_orbit_number:04d}"
            Data[key]['IW{subswath_num}'].append(burst)

# 5. Optional track/pass filter
if track_filter:
    Data = {k: v for k, v in Data.items()
            if any(str(t) in k for t in track_filter)
               and any(p in k for p in pass_filter)}
```

### L3 tile detection

Uses the tile generation algorithm from [L3 Grid System](#l3-grid-system). For each tile whose EPSG:4326 polygon intersects the ROI, the tile is added to `DataL3`.

### Download list construction (`updatelist`)

```python
# L2a/L2b
for track_key in Data:
    for iw in ['1', '2', '3']:
        for burst in Data[track_key][f'IW{iw}']:
            name = f"EGMS_{level}_{burst['relative_orbit_number']:03d}_{burst['egms_burst_id']:04d}_IW{iw}_VV{suffix}.zip"
            url  = f"https://egms.land.copernicus.eu/insar-api/archive/download/{name}"
            listL2a.append(name)
            listL2alink.append(url)

# L3
for tile in DataL3['polyL3']:
    x = tile.exterior.coords.xy[0][0] / 100000
    y = tile.exterior.coords.xy[1][0] / 100000
    name = f"EGMS_L3_E{int(y):2d}N{int(x):2d}_100km_{component}{suffix}.zip"
    url  = f"https://egms.land.copernicus.eu/insar-api/archive/download/{name}"

# Deduplicate (all four lists)
listL2a = list(numpy.unique(listL2a))
```

---

## Download Engine

### Parallelism

```
nbworker = 1:  sequential loop in main thread
nbworker > 1:  joblib.Parallel(n_jobs=nbworker, backend='threading')
               → threads share memory, no GIL issue for I/O
               → no tqdm progress bar per file (would interleave)
               → _ProgressCounter prints one line per completed file
```

### Token round-robin

```python
for i, (filename, link) in enumerate(all_files):
    token = tokens[i % len(tokens)]
    work_items.append((link, output_path, filename, token))
```

### _RateLimiter (shared cooldown)

```python
class _RateLimiter:
    _lock      threading.Lock
    _resume_at float    # monotonic timestamp, initially 0

    def signal(self, wait_seconds):
        deadline = time.monotonic() + wait_seconds
        with self._lock:
            if deadline > self._resume_at:
                self._resume_at = deadline

    def wait_if_needed(self):
        with self._lock:
            remaining = self._resume_at - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
```

One `_RateLimiter` instance created per `download()` call, shared across all workers.

### _ProgressCounter (parallel progress)

```python
class _ProgressCounter:
    _lock  threading.Lock
    _done  int
    total  int

    def increment(self, filename, status_char):
        # '+' = downloaded, '=' = already on disk, 'x' = failed/skipped
        with self._lock:
            self._done += 1
        print(f'[{status_char}] {self._done} / {self.total} : {filename}')
```

### Per-file download loop

```python
def _download_one_file(work_item, log, verbose, rate_limiter, max_retries=4, retry_wait=5, progress=None):
    url_base, output_path, filename, token = work_item
    url = f'{url_base}?id={token}'

    for attempt in range(max_retries):
        rate_limiter.wait_if_needed()

        existing_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
        headers = {'Range': f'bytes={existing_size}-'}

        try:
            response = requests.get(url, headers=headers, stream=True,
                                    allow_redirects=True, timeout=(5, 5))
        except Exception as e:
            # Connection error → signal cooldown, continue
            wait_time = min(retry_wait * (2 ** attempt), 60)
            rate_limiter.signal(wait_time)
            continue

        status = response.status_code

        if status in (429, 502):
            wait_time = min(retry_wait * (2 ** attempt), 300)
            rate_limiter.signal(wait_time)
            rate_limiter.wait_if_needed()
            continue

        if status == 416:
            return (filename, True, None)     # already complete

        if status not in (200, 206):
            return (filename, False, f'HTTP {status}')

        # Stream to disk
        mode = 'ab' if existing_size > 0 else 'wb'
        with open(output_path, mode) as f:
            for chunk in response.iter_content(chunk_size=8184):
                if chunk:
                    f.write(chunk)

        return (filename, True, None)

    # Exhausted retries
    if not os.path.exists(output_path):
        return (filename, False, 'file not available on server')
    return (filename, False, 'max retries exceeded')
```

---

## Post-Processing

### CSV merge (`datamergingcsv`)

**Inputs:** All CSV files under `{outputdir}/*/*/*.csv`  
**Grouping:** by level + track (e.g. `L2a_0124`)  
**Header detection:**
- Parameter columns: columns NOT containing `'20'`
- Time-series columns: columns containing `'20'` (YYYYMMDD dates)

**Output:** `{outputdir}/{level}_{track}.csv` — semicolon-separated, EPSG:3035

### TIFF merge (`datamergingtiff`)

**Inputs:** All `.tiff` files under `{outputdir}/*/*/*.tiff`  
**Command:**

```bash
# Linux/Mac:
gdal_merge.py -o {outputdir}/{name}.tiff -n -9999 -a_nodata -9999 file1.tiff file2.tiff ...

# Windows:
gdal_merge -o {outputdir}/{name}.tiff -n -9999 -a_nodata -9999 file1.tiff file2.tiff ...
```

### Gridding / interpolation (`datagridding`)

Converts L2 point CSV to raster GeoTIFF.

**Command (CSV input):**
```bash
gdal_grid \
  -zfield "{variable}" \
  -a_srs EPSG:3035 \
  -oo HEADERS=YES \
  -oo SEPARATOR=SEMICOLON \
  -oo X_POSSIBLE_NAMES=easting \
  -oo Y_POSSIBLE_NAMES=northing \
  -a {algorithm} \
  -txe {Xmin} {Xmax} \
  -tye {Ymin} {Ymax} \
  -tr {xres} {yres} \
  -of GTiff \
  -l {layer} \
  -ot Float64 \
  {input}.csv {output}_{variable}.tif
```

**Command (VRT input):** same but replace `-oo` flags with `-of GTiff` only, and `.csv` with `.vrt`.

**Post-metadata fix:**
```bash
gdal_edit.py -mo AREA_OR_POINT=Point {output}.tif --config GTIFF_POINT_GEO_IGNORE YES
```

**Algorithms:** `invdist`, `invdistnn`, `average`, `nearest`, `linear`

### CSV clipping (`dataclipping`)

**Method (pure Python):**
```python
# 1. Read bbox.shp → Polygon in EPSG:3035
# 2. For each row in CSV:
#      pt = Point(easting, northing)
#      if polygon.contains(pt): write row
# Output: {name}_clipped.csv
```

**Method (ogr2ogr, optional):**
```bash
ogr2ogr \
  -of CSV \
  -clipsrc {bbox_poly_3035.shp} \
  -s_srs EPSG:3035 \
  -t_srs EPSG:3035 \
  -oo HEADERS=YES \
  -oo SEPARATOR=SEMICOLON \
  -oo X_POSSIBLE_NAMES=easting \
  -oo Y_POSSIBLE_NAMES=northing \
  {output}_clipped.csv {input}.csv
```

### TIFF cropping (`dataclipping` for TIFF)

```bash
rio mask {input}.tiff {output}_cropped.tiff \
  --crop \
  --geojson-mask {bbox_3035.geojson} \
  --overwrite
```

### CSV → vector conversion (`convertcsv`)

```bash
ogr2ogr \
  -of {format} \
  -s_srs EPSG:3035 \
  -t_srs EPSG:3035 \
  -oo HEADERS=YES \
  -oo SEPARATOR=SEMICOLON \
  -oo X_POSSIBLE_NAMES=easting \
  -oo Y_POSSIBLE_NAMES=northing \
  {output}.{ext} {input}.csv
```

Supported formats: `ESRI Shapefile`, `GPKG`, `GeoJSON`

---

## Error Handling & Retry Logic

### Per-file retry

```
max_retries = 4
retry_wait  = 5  (base seconds)

attempt 0: wait = 5   × 2^0 = 5s    (max 300s)
attempt 1: wait = 5   × 2^1 = 10s
attempt 2: wait = 5   × 2^2 = 20s
attempt 3: wait = 5   × 2^3 = 40s
→ give up
```

### Outcome classification

| Condition | Classification |
|-----------|---------------|
| HTTP 200/206, file written | `downloaded` |
| HTTP 416 | `downloaded` (already complete) |
| HTTP 429/502, retries exhausted, no file on disk | `missing` |
| HTTP 429/502, retries exhausted, partial file exists | `failed` |
| Other HTTP error or exception | `failed` |

### Shared rate limiter behaviour

When worker A hits 429 or 502:
1. Calls `rate_limiter.signal(wait_time)` — sets global deadline
2. Calls `rate_limiter.wait_if_needed()` — sleeps until deadline
3. All other workers check `wait_if_needed()` before each request → also sleep

No blocking Event — uses lock-protected monotonic timestamp. No deadlock possible.

---

## Token & Authentication

### Token format

- 32-character hexadecimal string
- Obtained from EGMS website user account
- Temporary / session-based (may expire)

### URL injection

```
{base_url}{filename}?id={token}
```

Token is a query parameter, visible in logs and URLs.

### Multi-token support

```python
# Accepted inputs (all normalised to list[str]):
"TOKEN1"                         → ['TOKEN1']
"TOKEN1,TOKEN2,TOKEN3"           → ['TOKEN1', 'TOKEN2', 'TOKEN3']
['TOKEN1', 'TOKEN2']             → ['TOKEN1', 'TOKEN2']

# Assignment to work items (round-robin):
token = tokens[file_index % len(tokens)]
```

### Backward compatibility

`downloader.token` property returns `tokens[0]`.  
Setting `downloader.token = 'X'` replaces `tokens` with `['X']`.

---

## Constants

```python
CHUNK_SIZE         = 8184          # bytes per streaming chunk (4092 * 2)
LOGGING_MODE       = 'INFO'        # Python logging level
PATH_S1_MAP        = '{package}/3rdparty'   # burst ID map storage
UNLOCK_FOLIUM      = False         # enable extra map tile providers
MAX_DOWNLOAD_RETRIES = 4
BASE_RETRY_WAIT    = 5             # seconds
MAX_RETRY_WAIT_429 = 300           # seconds (5 minutes)
MAX_RETRY_WAIT_CONN = 60           # seconds (1 minute)
GRID_X_MIN         = 900000        # EPSG:3035 bounds for L3 grid
GRID_X_MAX         = 7400000
GRID_Y_MIN         = 900000
GRID_Y_MAX         = 7400000
GRID_TILE_SIZE     = 100000        # 100 km
GRID_BUFFER        = 5             # extra tiles
```

---

## External Dependencies

### Python packages

| Package | Purpose |
|---------|---------|
| `numpy` | Array ops, deduplication |
| `pandas` | CSV merging |
| `scipy` | Spatial ops |
| `shapely` | Geometry (Polygon, intersection tests) |
| `fiona` | Read shapefiles, SQLite burst DB |
| `pyproj` | CRS transformations (EPSG:4326 ↔ EPSG:3035) |
| `rasterio` | TIFF cropping (`rio mask`) |
| `gdal` (osgeo) | Raster operations |
| `requests` | HTTP downloads |
| `urllib3` | Underlying HTTP |
| `joblib` | Parallel unzip + parallel download |
| `threading` | `Lock`, shared state for rate limiter |
| `folium` | Interactive HTML maps |
| `plotly` | Static map fallback |
| `Pillow` | Save folium map as JPEG |
| `tqdm` | Progress bar (single-worker downloads) |
| `alive_progress` | General progress display |
| `concave_hull` | Duplicate point removal in CSV merge |
| `kaleido` | Plotly image export |

### CLI tools (called via subprocess)

| Tool | Purpose | Platform |
|------|---------|----------|
| `gdal_merge.py` / `gdal_merge` | Mosaic TIFF tiles | Linux: `.py` suffix; Windows: no suffix |
| `gdal_grid` | Interpolate CSV points → raster | Both |
| `gdal_edit.py` / `gdal_edit` | Edit GeoTIFF metadata | Both |
| `ogr2ogr` | Vector format conversion, clipping | Both |
| `rio` (rasterio CLI) | TIFF masking/cropping | Both |
| `gmt coast` | Extract country boundary polygon | Both (optional) |

### Platform detection for GDAL

```python
import platform
key = '' if platform.system() == 'Windows' else '.py'
# Usage:
cmd = f'gdal_merge{key} -o output.tiff ...'
```
