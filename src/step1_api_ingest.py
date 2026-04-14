import requests
import pandas as pd
import json
url = "https://api.fda.gov/drug/ndc.json?limit=10"
response = requests.get(url)
print(f"Status Code: {response.status_code}")
data = response.json()
print(json.dumps(data, indent=2)[:500])
api_records = data["results"]
print(f"Records fetched: {len(api_records)}")

clean_records = []
for item in api_records:
    clean_records.append({
        "drug_name":item.get("generic_name", "Unknown"),
        "brand_name":item.get("brand_name", "Unknown"),
        "dosage_form":item.get("dosage_form", "Unknown"),
        "product_type":item.get("product_type", "Unknown"),
    })

df = pd.DataFrame(clean_records)
print(f"\nShape: {df.shape}")
print(df)

from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:password@localhost:5432/healthcare_db")
df.to_sql("drug_events", engine, if_exists="replace", index=False)
print(f"\nSaved {len(df)} records to PostgreSQL!")