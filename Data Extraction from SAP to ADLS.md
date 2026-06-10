# Data Extraction from SAP (Custom CDS) to ADLS

## Custom CDS views in SAP

- Open Custom CDS : ```Custom CDS Views - S/4HANA Cloud```
- Search whether the CDS name is present in the list or not
	- if not present then only proceed
- Create CDS view with the CDS name but select Scenario as External API
- Find the CDS name similar to your CDS view, 
	- Select the most appropriate data source
- Go to Elements, add the Elements (columns)
- Select All the elements, but later remove the ones marked in Red (those are Associations)
- Click on Publish
- Search for ```Custom Communication Scenario```
- Select the Communication scenario based on the Description (the most approproate one)
- Edit and add the CDS view you created, Save it
- Then Again Click Publish. Wait for it to get Published

### For Verification:
- Go to Communication Arrangement 
- Open Communication Arrangement
- Check if the Custom CDS view is present in the list along with an URL


## Databricks Code (SAP &rarr ADLS)

### Import all libraries
```
import json
import base64
import csv
import io
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import to_json, col
```
 
### Spark session + dbutils safety
```
spark = SparkSession.builder.getOrCreate()
try:
    dbutils  # noqa: F821
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
```
 
### AUTH (keep your working user)
```
# Prefer secrets:
# SAP_USERNAME = dbutils.secrets.get(scope="sap-scope", key="sap-username")
# SAP_PASSWORD = dbutils.secrets.get(scope="sap-scope", key="sap-password")
 
# Hardcoded 
SAP_USERNAME = "Fill your username"
SAP_PASSWORD = "Fill your password"
``` 


### Landing Path
```
LANDING_PATH = "/mnt/landing_zone/<your landing path>/"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
PAGE_SIZE = 5000
 
dbutils.fs.mkdirs(LANDING_PATH)
```
 
### YOUR Custom CDS view URLs
```
SERVICE_URLS = {
    "<custom_cds_view_1>" : "<url_1>",
	"<custom_cds_view_2>" : "<url_2>" 
}
# add accordingly
```

### Helpers: derive SAP_BASE_URL and service name from URL
``` 
def normalize_url(u: str) -> str:
    # remove accidental pasted tail like "CDSTHIS..."
    u = (u or "").strip()
    # keep only up to _CDS if extra chars appear
    m = re.search(r"(https?://.*?/sap/opu/odata/sap/[^/\s]+)", u)
    return m.group(1) if m else u
 
 
def extract_base_url(full_url: str) -> str:
    # e.g. https://my412186-api.s4hana.cloud.sap
    m = re.match(r"^(https?://[^/]+)", full_url)
    if not m:
        raise ValueError(f"Invalid URL: {full_url}")
    return m.group(1)
 
 
def extract_service_name(full_url: str) -> str:
    # e.g. .../sap/opu/odata/sap/YY1_XXXX_CDS -> YY1_XXXX_CDS
    m = re.search(r"/sap/opu/odata/sap/([^/?#]+)", full_url)
    if not m:
        raise ValueError(f"Could not extract service name from URL: {full_url}")
    return m.group(1)
 
 
# normalize all URLs
SERVICE_URLS = {k: normalize_url(v) for k, v in SERVICE_URLS.items()}
SAP_BASE_URL = extract_base_url(next(iter(SERVICE_URLS.values())))
```
 
### HTTP session with retries + preemptive basic header
```
def build_session():
    s = requests.Session()
 
    token = base64.b64encode(f"{SAP_USERNAME}:{SAP_PASSWORD}".encode("utf-8")).decode("utf-8")
 
    s.headers.update({"Authorization": f"Basic {token}"})
    s.auth = (SAP_USERNAME, SAP_PASSWORD)
 
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
 
    retry = Retry(
        total=5,
        backoff_factor=2, # changed 1 -> 2
        status_forcelist=[400, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s
 
session = build_session()
```

### Entity-set discovery from service doc
``` 
def get_entity_sets_from_service_doc(service_name):
    service_url = f"{SAP_BASE_URL}/sap/opu/odata/sap/{service_name}/"
    headers = {"Accept": "application/atomsvc+xml,application/xml,*/*;q=0.8"}
 
    r = session.get(service_url, headers=headers, timeout=60)
 
    if r.status_code == 406:
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        r = session.get(service_url, headers=headers, timeout=60)
 
    print(f"Trying to fetch {service_name}....")
    r.raise_for_status()
    print(f"Fetched {service_name}")
 
    root = ET.fromstring(r.text)
    ns = {"app": "http://www.w3.org/2007/app", "atom": "http://www.w3.org/2005/Atom"}
 
    entity_sets = []
    for c in root.findall(".//app:collection", ns):
        href = c.attrib.get("href") # extracting labels from XML
        if href:
            entity_sets.append(href)
 
    if not entity_sets:
        raise Exception(f"No EntitySets found in service document for {service_name}")
 
    return entity_sets
 
 
 
def choose_best_entity_set(entity_sets):
    bad_tokens = ["valuehelp", "vh", "text", "parameters", "f4", "help"]
 
    def score(name):
        s = 100
        lname = name.lower()
        for t in bad_tokens:
            if t in lname:
                s -= 30
        return s
 
    return sorted(entity_sets, key=score, reverse=True)[0]
 
 
 
def get_entity_set(service_name):
    sets = get_entity_sets_from_service_doc(service_name)
    print(f"Entity sets for service_name ({service_name}) are : {sets}")
    return sets[0] if len(sets) == 1 else choose_best_entity_set(sets)
```

### Fetch OData V2 (pagination via d.__next)
```
def fetch_odata_v2_all(service_name, page_size=5000):
    entity_set = get_entity_set(service_name)
    base_url = f"{SAP_BASE_URL}/sap/opu/odata/sap/{service_name}/{entity_set}" # API URL
 
    print(f"Fetching service = {service_name}, entity_set={entity_set}")
 
    records = []
    url = base_url
    params = {"$format": "json", "$top": page_size, "$skip": 0}
    headers = {"Accept": "application/json"}
 
    while True:
        resp = session.get(url, params=params, headers=headers, timeout=120)
        resp.raise_for_status()
        payload = resp.json()
 
        d = payload.get("d", {})
        batch = d.get("results", [])
        records.extend(batch)
 
        next_link = d.get("__next")
        if not next_link:
            break
 
        url = (SAP_BASE_URL + next_link) if next_link.startswith("/") else next_link
        params = None
 
    return entity_set, records
```
 
### Safe move (remove destination if exists)
```
def safe_mv(src, dst):
    try:
        dbutils.fs.rm(dst)
    except Exception:
        pass
    dbutils.fs.mv(src, dst)
```
 
### Process & Save CSV (single file) + Row Count
```
def process_and_save(entity_name, records):
    if not records:
        print(f"No records for {entity_name}")
        return "", 0
 
    # row count
    row_count = len(records)
    print(f" {entity_name} rows: {row_count}")
 
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
    df = spark.read.json(rdd)
 
    # Convert nested columns to JSON strings (CSV-safe)
    for f in df.schema.fields: # looping through all cols
        if isinstance(f.dataType, (StructType, ArrayType)):
            df = df.withColumn(f.name, to_json(col(f.name)))
 
    temp_path = f"{LANDING_PATH}temp_{entity_name}_{TIMESTAMP}"
    final_path = f"{LANDING_PATH}{entity_name}_{TIMESTAMP}.csv"
   
    # write the csv
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)
 
    # find the actual file
    part_files = [f.path for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")]
    if not part_files:
        raise Exception(f"No part file found in {temp_path}")
 
    safe_mv(part_files[0], final_path)
    dbutils.fs.rm(temp_path, recurse=True)
 
    print(f"Saved: {final_path}")
    return final_path, row_count
```
 

### Manifest writer (NO Spark write; stable)
```
def write_manifest(manifest_rows, out_dir, timestamp):
    if not manifest_rows:
        return ""
 
    output = io.StringIO() # temporary string buffer
    writer = csv.DictWriter(    # prepares csv writer
        output,
        fieldnames=[
            "RunTimestamp", "EntityName", "FullServiceURL", "ServiceName", "EntitySet",
            "RowCount", "OutputFilePath", "Status", "ErrorMessage"
        ]
    )
    writer.writeheader()
 
    for r in manifest_rows:
        writer.writerow(r)
 
    manifest_path = f"{out_dir}P2P_INGEST_MANIFEST_{timestamp}.csv"
    dbutils.fs.put(manifest_path, output.getvalue(), overwrite=True)    # saves csv to datalake
    print(f"Manifest saved: {manifest_path}")
    return manifest_path
```

### main() function

```
manifest_rows = []
run_ts_iso = datetime.now().isoformat()
 
for entity_name, full_url in SERVICE_URLS.items():
    row = {
        "RunTimestamp":     run_ts_iso,
        "EntityName":       entity_name,
        "FullServiceURL":   full_url,
        "ServiceName":      "",
        "EntitySet":        "",
        "RowCount":         0,
        "OutputFilePath":   "",
        "Status":           "FAILED",
        "ErrorMessage":     ""
    }
 
    try:
        service_name = extract_service_name(full_url)
        row["ServiceName"] = service_name
 
        entity_set, data = fetch_odata_v2_all(service_name, page_size=PAGE_SIZE)
        row["EntitySet"] = entity_set
 
        final_path, cnt = process_and_save(entity_name, data)
        row["RowCount"] = cnt
        row["OutputFilePath"] = final_path
        row["Status"] = "SUCCESS"
 
    except Exception as e:
        row["ErrorMessage"] = str(e)
        print(f"Error for {entity_name} ({full_url}): {row['ErrorMessage']}")
 
    manifest_rows.append(row)
    print("-"*50,"\n")
 
manifest_file = write_manifest(manifest_rows, LANDING_PATH, TIMESTAMP)
print("Data Ingestion completed.")
print(f"Manifest: {manifest_file}")
```

 
