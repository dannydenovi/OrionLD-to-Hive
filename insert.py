import os
import glob
import pandas as pd
import happybase
from datetime import datetime, timedelta
import random

HBASE_HOST = "localhost"
COLUMN_FAMILY = "cf"
TARGET_DIR = "./Measurements"
LOAD_PERCENTAGE = 0.5

ENTITY_MAPPING = {
    "Room1": "room1_data",
    "Room2": "room2_data",
    "Room3": "room3_data",
    "Kitchen": "kitchen_data",
    "Bathroom": "bathroom_data",
    "Toilet": "toilet_data"
}

SENSOR_TYPES = {
    "temperature": "temperature",
    "humidity": "humidity",
    "brightness": "brightness"
}

def random_timestamp_within_range():
    base = datetime.now()
    offset_days = random.randint(0, 5)
    random_time = base + timedelta(days=offset_days, hours=random.randint(0, 23), minutes=random.randint(0, 59),
                                   seconds=random.randint(0, 59), microseconds=random.randint(0, 999999))
    return random_time.strftime('%Y-%m-%d %H:%M:%S.%f')

def infer_entity_and_sensor(filename):
    entity = None
    sensor = None
    lower_name = filename.lower()
    for e in ENTITY_MAPPING:
        if e.lower() in lower_name:
            entity = e
            break
    for s in SENSOR_TYPES:
        if s in lower_name:
            sensor = SENSOR_TYPES[s]
            break
    return entity, sensor

def reset_table(connection, table_name):
    encoded_name = table_name.encode()
    if encoded_name in connection.tables():
        try:
            connection.disable_table(encoded_name)
            connection.delete_table(encoded_name)
            print(f"🧹 Deleted table: {table_name}")
        except Exception as e:
            print(f"❌ Failed to delete table {table_name}: {e}")
    try:
        connection.create_table(encoded_name, {COLUMN_FAMILY: dict()})
        print(f"✅ Created table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")

def insert_csv_to_hbase(file_path, connection):
    filename = os.path.basename(file_path)
    entity, sensor = infer_entity_and_sensor(filename)
    if not entity or not sensor:
        print(f"❌ Impossibile inferire entità o tipo sensore da {filename}")
        return

    table_name = ENTITY_MAPPING[entity]
    table = connection.table(table_name.encode())

    df = pd.read_csv(file_path, sep='\t', header=None, names=["timestamp", "value"])
    print(f"📄 File: {filename} | Original rows: {len(df)}")

    df = df.dropna(how='any')

    if LOAD_PERCENTAGE < 1.0:
        df = df.sample(frac=LOAD_PERCENTAGE, random_state=42)
        print(f"🔍 After sampling: {len(df)} rows")

    for i, row in df.iterrows():
        ts = random_timestamp_within_range()
        rowkey = f"{entity}_{ts}_{i}".encode()

        data = {
            b'cf:timestamp': ts.encode(),
            f"cf:{sensor}".encode(): str(row["value"]).encode()
        }

        try:
            table.put(rowkey, data)
        except Exception as e:
            print(f"❌ Failed to insert row {rowkey}: {e}")

    print(f"✅ Inseriti {len(df)} record da {filename} nella tabella {table_name}")

def main():
    connection = happybase.Connection(HBASE_HOST)

    # reset all target tables once before insertion
    for table in ENTITY_MAPPING.values():
        reset_table(connection, table)

    csv_files = glob.glob(os.path.join(TARGET_DIR, "*.csv"))
    for file_path in csv_files:
        insert_csv_to_hbase(file_path, connection)

    connection.close()

if __name__ == "__main__":
    main()
