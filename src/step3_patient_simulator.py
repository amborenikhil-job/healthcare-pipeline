import random
import time
import json
from datetime import datetime
def generate_patient():
    patient_id = random.randint(1000, 9999)
    age = random.randint(18, 90)
    heart_rate = random.randint(55, 120)
    systolic_bp = random.randint(90, 180)
    diastolic_bp = random.randint(60, 110)
    oxygen_level = round(random.uniform(92, 100), 1)
    temperature = round(random.uniform(97.0, 103.0), 1)

    return {
        "patient_id": patient_id,
        "age": age,
        "heart_rate": heart_rate,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "oxygen_level": oxygen_level,
        "temperature": temperature,
        "timestamp": datetime.now().isoformat()
    }
def check_alerts(patient):
    alerts = []
    if patient["heart_rate"] > 100:
        alerts.append("High heart rate")
    if patient["heart_rate"] < 60:
        alerts.append("Low heart rate")
    if patient["systolic_bp"] > 140:
        alerts.append("High blood pressure")
    if patient["oxygen_level"] < 95:
        alerts.append("Low oxygen")
    if patient["temperature"] > 100.4:
        alerts.append("Fever")
    patient["alerts"] = alerts
    patient["is_critical"] = len(alerts) > 0
    return patient
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("Starting patient stream... Press Ctrl+C to stop\n")
while True:
    patient = generate_patient()
    patient = check_alerts(patient)
    producer.send("patient-vitals", patient)

    status = "CRITICAL" if patient["is_critical"] else "normal"
    print(f"Patient {patient['patient_id']} | HR: {patient['heart_rate']} | BP: {patient['systolic_bp']}/{patient['diastolic_bp']} | O2: {patient['oxygen_level']}% | Temp: {patient['temperature']}°F | Status: {status}")

    if patient["alerts"]:
        print(f" ALERTS: {', '.join(patient['alerts'])}")

    time.sleep(1)
    

