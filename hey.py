import random
import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

NUM_ROWS = 1000

def random_lat():
    return random.uniform(-90, 90)

def random_lon():
    return random.uniform(-180, 180)

def random_timestamp(start):
    return start + timedelta(seconds=random.randint(60, 7200))

def random_dem_version():
    versions = ["1.0", "1.1", "2.0", "2.1", None]
    return random.choice(versions)

def random_terminate_reason():
    reasons = [
        "NORMAL",
        "APP_CLOSED",
        "GPS_LOST",
        "IDLE_TIMEOUT",
        "BATTERY_LOW"
    ]
    return random.choice(reasons)

def random_org():
    orgs = ["ORG-123", "ORG-456", "ORG-789", "ORG-ABC"]
    return random.choice(orgs)

def get_reject_reason(terminate_reason):

    if terminate_reason == "NORMAL":
        return "NONE"

    elif terminate_reason == "GPS_LOST":
        return random.choice(["GPS_POOR", "NONE"])

    elif terminate_reason == "APP_CLOSED":
        return random.choice(["NONE", "TOO_SHORT"])

    elif terminate_reason == "IDLE_TIMEOUT":
        return random.choice(["NONE", "INVALID_DATA"])

    elif terminate_reason == "BATTERY_LOW":
        return random.choice(["TOO_SHORT", "NONE"])

    return "NONE"

def random_mobile_info():

    ios_devices = [
        ("iPhone 15", "17.4"),
        ("iPhone 14", "17.2"),
        ("iPhone 13", "16.6"),
        ("iPhone 12", "16.4")
    ]

    android_devices = [
        ("Samsung Galaxy S23", "14"),
        ("Samsung Galaxy S22", "13"),
        ("Pixel 8", "14"),
        ("Pixel 7", "13"),
        ("OnePlus 11", "14")
    ]

    if random.random() < 0.5:
        device, os_version = random.choice(ios_devices)
        os_name = "iOS"
    else:
        device, os_version = random.choice(android_devices)
        os_name = "Android"

    app_versions = ["5.0.0", "5.1.0", "5.1.2", "5.2.0", "5.3.1"]

    return {
        "mobileAppDevice": device,
        "mobileAppVersion": random.choice(app_versions),
        "mobileOsVersion": os_version,
        "mobileOs": os_name
    }

def random_security_status():
    return random.choices(
        ["true", "false", None],
        weights=[0.85, 0.1, 0.05]
    )[0]

def generate_distance_calculations(start_time):

    num_segments = random.randint(1,4)

    calculations = []

    for i in range(num_segments):

        segment_start = start_time + timedelta(seconds=i*60)

        calculations.append({
            "haversineDistance": random.uniform(0.1, 2.0),
            "integrationDistance": random.uniform(0.1, 2.0),
            "intervalDuration": random.randint(10,60),
            "timeStart": segment_start.isoformat()
        })

    return calculations

def generate_feature_support():

    return {
        "PhoneStatePermission": None,
        "accel": random.choice([0,1]),
        "baro": random.choice([0,1]),
        "gravity": random.choice([0,1]),
        "gyro": random.choice([0,1]),
        "isSecurityEnabled": random.choice([0,1]),
        "motionFitnessPermission": random.choice([True, False])
    }

def random_config_id():

    configs = [
        "CFG-BASELINE",
        "CFG-DRIVER-RISK-V1",
        "CFG-DRIVER-RISK-V2",
        "CFG-TELEMATICS-ADV",
        None
    ]

    return random.choice(configs)


def generate_geo_points(start_time, start_lat, start_lon, points=100):

    geo = []
    current_time = start_time

    lat = start_lat
    lon = start_lon

    total_distance = 0

    for i in range(points):

        lat += random.uniform(-0.001, 0.001)
        lon += random.uniform(-0.001, 0.001)

        current_time += timedelta(seconds=random.randint(1, 5))

        distance = random.uniform(0.001, 0.05)
        total_distance += distance

        geo.append({
            "accuracy": random.uniform(5, 20),
            "bearing": random.uniform(0, 360),
            "gpsEpoch": int(current_time.timestamp()),
            "latitude": lat,
            "longitude": lon,
            "speed": random.uniform(0, 80),
            "timestamp": current_time.isoformat(),
            "pointToPointHaversineDistance": distance
        })

    return geo, total_distance

def generate_event_details(start_time, end_time, start_lat, start_lon):

    events = []

    num_events = random.randint(0, 5)

    for _ in range(num_events):

        event_start = start_time + timedelta(seconds=random.randint(0, 300))
        event_end = event_start + timedelta(seconds=random.randint(5, 30))

        events.append({
            "type": random.randint(1, 5),
            "sampleSpeed": random.uniform(0, 90),
            "speedChange": random.uniform(-20, 20),
            "sensorDetectionMethod": random.choice(["GPS","MEMS",None]),
            "duration": (event_end - event_start).seconds,
            "startTimestamp": event_start.isoformat(),
            "endTimestamp": event_end.isoformat(),
            "eventStart_epoch": int(event_start.timestamp()),
            "eventEnd_epoch": int(event_end.timestamp()),
            "milesDriven": random.uniform(0.01, 1),
            "gpsSignalStrength": random.randint(1,5),
            "startLatitude": start_lat,
            "startLongitude": start_lon,
            "endLatitude": start_lat + random.uniform(-0.001,0.001),
            "endLongitude": start_lon + random.uniform(-0.001,0.001),
            "modelConfigID": str(uuid.uuid4()),
            "modelConfigStatusCode": random.randint(0,5),
            "modelConfigExceptionCodes": [],
            "modelConfigOutput": [random.random() for _ in range(3)],
            "eventOutput": [random.random() for _ in range(3)],
            "eventConfidence": random.uniform(0,1),
            "eventId": str(uuid.uuid4()),
            "supplementalEventDetails": None
        })

    return events


def generate_histogram():

    bins = ["0-10","10-20","20-30","30-40","40-50","50+"]

    return [
        {
            "secCount": random.uniform(0,100),
            "speedRangeCd": b
        }
        for b in bins
    ]


def generate_event_histogram():

    bins = ["0-10","10-20","20-30","30-40","40-50","50+"]

    return [
        {
            "eventCount": random.randint(0,10),
            "speedRangeCd": b
        }
        for b in bins
    ]


records = []

for _ in range(NUM_ROWS):

    start_time = datetime.now() - timedelta(days=random.randint(0,30))
    end_time = random_timestamp(start_time)

    start_lat = random_lat()
    start_lon = random_lon()

    end_lat = start_lat + random.uniform(-0.01,0.01)
    end_lon = start_lon + random.uniform(-0.01,0.01)

    geo_points, total_miles = generate_geo_points(start_time,start_lat,start_lon)
    terminate_reason = random_terminate_reason()
    reject_reason = get_reject_reason(terminate_reason)

    mobile_info = random_mobile_info()

    trip = {

        "tripId": str(uuid.uuid4()),
        "userId": str(uuid.uuid4()),

        "tripStartLatitude": start_lat,
        "tripEndLatitude": end_lat,

        "tripStartLongitude": start_lon,
        "tripEndLongitude": end_lon,

        "tripStartTimestamp": start_time.isoformat(),
        "tripEndTimestamp": end_time.isoformat(),

        "tripStart_epoch": int(start_time.timestamp()),
        "tripEnd_epoch": int(end_time.timestamp()),

        "distance": total_miles,
        "totalTripMiles": total_miles,

        "tripMilesTime_1": total_miles*0.2,
        "tripMilesTime_2": total_miles*0.2,
        "tripMilesTime_3": total_miles*0.2,
        "tripMilesTime_4": total_miles*0.2,
        "tripMilesTime_5": total_miles*0.2,

        "duration": (end_time-start_time).seconds,

        "deviceId": str(uuid.uuid4()),

        "idleTime": random.uniform(0,300),

        "demVersion": random_dem_version(),

        "tripTerminateId": str(uuid.uuid4()),
        "tripTerminateReasonCd": terminate_reason,
    
        "hostSDK": "arity-sdk",

        "eventDetails": generate_event_details(start_time,end_time,start_lat,start_lon),

        "systemEvents": None,

        "geoPoints": geo_points,

        "accelerationEventsHistogram": generate_event_histogram(),
        "brakeEventsHistogram": generate_event_histogram(),

        "accelerationHistogram": generate_histogram(),
        "brakeHistogram": generate_histogram(),
        "speedHistogram": generate_histogram(),

        "milesDrivenByHour":[
            {"miles": random.uniform(0,5),"hour": str(h)}
            for h in range(24)
        ],

        "organizationId": random_org(),

        "averageSpeed": random.uniform(10,70),
        "maxSpeed": random.uniform(70,120),

        "tripRejectReasonCd": reject_reason,

        "tripUploadTimestamp": datetime.now().isoformat(),
        "tripRemoveTimestamp": None,

        "milesAtOrOverMaxSpeed": random.uniform(0,2),

        "mobileAppDevice": mobile_info["mobileAppDevice"],
        "mobileAppVersion": mobile_info["mobileAppVersion"],
        "mobileOsVersion": mobile_info["mobileOsVersion"],
        
        "tripProcessed_TS": datetime.now().isoformat(),

        "locale":"en-US",

        "isSecurityEnabled": random_security_status(),

        "startTripBatteryLevel": random.uniform(0.3,1),
        "endTripBatteryLevel": random.uniform(0.1,0.9),

        "mobileOs": mobile_info["mobileOs"],

        "distanceCalculations": generate_distance_calculations(start_time),
        "featureSupport": generate_feature_support(),
        "configId": random_config_id()
    }

    records.append(trip)


df = pd.DataFrame(records)

df.to_json("ctf_trip_summary_1000.json", orient="records", lines=True)
df.to_csv("ctf_trip_summary_1000.csv", index=False)

table = pa.Table.from_pandas(df, preserve_index=False)

pq.write_table(table,"ctf_trip_summary_1000.parquet")

print("Parquet file generated: ctf_trip_summary_1000.parquet")
