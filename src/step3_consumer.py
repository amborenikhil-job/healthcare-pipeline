import json
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from datetime import datetime
consumer = KafkaConsumer(
    "patient-vitals",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

engine = create_engine("postgresql://postgres:password@localhost:5432/healthcare_db")
print("Listening for patient events...\n")

for message in consumer:
    patient = message.value
    df = pd.DataFrame([patient])
    df.to_sql("patient_vitals", engine, if_exists="append", index=False)

    if patient["is_critical"]:
        print(f"CRITICAL: Patient {patient['patient_id']} | {patient['alerts']}")
    else:
        print(f"OK: Patient {patient['patient_id']}")
        