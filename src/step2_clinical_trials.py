import requests
import pandas as pd
from sqlalchemy import create_engine
url = "https://clinicaltrials.gov/api/v2/studies?query.cond=diabetes&pageSize=20&format=json"
response = requests.get(url)
print(f"Status: {response.status_code}")
data = response.json()
trials = data["studies"]
print(f"Trials fetched: {len(trials)}")
print(f"First trial keys: {list(trials[0].keys())}")
first_trial = trials[0]["protocolSection"]
print(f"\nProtocol keys: {list(first_trial.keys())}")
clean_trails = []
for trial in trials:
    protocol = trial["protocolSection"]

    identification = protocol.get("identificationModule", {})
    status = protocol.get("statusModule", {})
    conditions = protocol.get("conditionsModule", {})
    design = protocol.get("designModule", {})

    clean_trails.append({
        "trial_id": identification.get("nctId", "Unknown"),
        "title": identification.get("briefTitle", "Unknown"),
        "status": status.get("overallStatus", "Unknown"),
        "start_date": status.get("startDateStruct", {}).get("date", "Unknown"),
        "condition": conditions.get("conditions", ["Unknown"])[0],
        "study_type": design.get("studyType", "Unknown"),
    })
df = pd.DataFrame(clean_trails)
print(f"\nShape: {df.shape}")
print(df[["trial_id", "status", "condition"]].head(10))
engine = create_engine("postgresql://postgres:password@localhost:5432/healthcare_db")
df.to_sql("clinical_trials", engine, if_exists="replace", index=False)
print(f"\nSaved {len(df)} trials to PostgreSQL!")