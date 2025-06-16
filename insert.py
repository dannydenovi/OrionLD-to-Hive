import os
import glob
import pandas as pd
import happybase
from datetime import datetime, timedelta
import random

HBASE_HOST = "localhost"
COLUMN_FAMILY = "cf"
TARGET_DIR = "./Measurements"  # <-- CAMBIA QUI
LOAD_PERCENTAGE = 0.25  # <-- CAMBIA QUI (es: 0.25 per 25%)

ENTITY_MAPPING = {
    "Room1": "room1_data",
    "Room2": "room2_data",
    "Room3": "room3_data",
    "Kitchen": "kitchen_data",
    "Bathroom": "bathroom_data",
    "Toilet": "toilet_data"
}

def random_timestamp_within_range():
    base = datetime.now()
    offset_days = random.randint(0, 5)
    random_time = base + timedelta(days=offset_days, hours=random.randint(0, 23), minutes=random.randint(0, 59),
                                   seconds=random.randint(0, 59), microseconds=random.randint(0, 999999))
    return random_time.strftime('%Y-%m-%d %H:%M:%S.%f')

def infer_entity_from_filename(filename):
    for entity in ENTITY_MAPPING:
        if entity.lower() in filename.lower():
            return entity
    return None

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
    entity = infer_entity_from_filename(filename)
    if not entity:
        print(f"❌ Impossibile inferire l'entità da {filename}")
        return

    table_name = ENTITY_MAPPING[entity]
    reset_table(connection, table_name)
    table = connection.table(table_name.encode())

    df = pd.read_csv(file_path)
    print(f"📄 File: {filename} | Original rows: {len(df)}")

    df = df.dropna(how='all')  # Rimuove righe completamente vuote

    if LOAD_PERCENTAGE < 1.0:
        df = df.sample(frac=LOAD_PERCENTAGE, random_state=42)
        print(f"🔍 After sampling: {len(df)} rows")

    for i, row in df.iterrows():
        ts = random_timestamp_within_range()
        rowkey = f"{entity}_{ts}_{i}".encode()

        data = {}
        if "temperature" in row and pd.notna(row["temperature"]):
            data[b'cf:temperature'] = str(row["temperature"]).encode()
        if "humidity" in row and pd.notna(row["humidity"]):
            data[b'cf:humidity'] = str(row["humidity"]).encode()
        if "brightness" in row and pd.notna(row["brightness"]):
            data[b'cf:brightness'] = str(row["brightness"]).encode()

        data[b'cf:timestamp'] = ts.encode()

        if data:
            try:
                table.put(rowkey, data)
            except Exception as e:
                print(f"❌ Failed to insert row {rowkey}: {e}")
        else:
            print(f"⚠️ Empty or invalid row at index {i}, skipped")

    print(f"✅ Inseriti {len(df)} record da {filename}")

def main():
    connection = happybase.Connection(HBASE_HOST)
    csv_files = glob.glob(os.path.join(TARGET_DIR, "*.csv"))
    for file_path in csv_files:
        insert_csv_to_hbase(file_path, connection)
    connection.close()

if __name__ == "__main__":
    main()
