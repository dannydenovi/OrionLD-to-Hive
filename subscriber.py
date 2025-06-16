from flask import Flask, request, jsonify
import requests, json
import happybase
from thriftpy2.thrift import TApplicationException
import threading
from datetime import datetime
import queue
import time
from collections import defaultdict

app = Flask(__name__)

ORION_LD_HOST = "http://192.168.1.23:1026"
SUBS_URL = f"{ORION_LD_HOST}/ngsi-ld/v1/subscriptions"
CALLBACK = "http://192.168.1.27:8000/notify"
SUB_ID = "urn:ngsi-ld:Subscription:KitchenUpdates"

HBASE_HOST = "localhost"
HBASE_TABLE = "kitchen_data"

write_buffer = queue.Queue()
last_sent = defaultdict(lambda: 0.0)
delay_seconds = 0.05

payload = {
    "id": SUB_ID,
    "type": "Subscription",
    "entities": [
        {"id": "urn:ngsi-ld:Kitchen:Kitchen", "type": "Kitchen"},
        {"id": "urn:ngsi-ld:Bathroom:Bathroom", "type": "Bathroom"},
        {"id": "urn:ngsi-ld:Room1:Room1", "type": "Room1"},
        {"id": "urn:ngsi-ld:Room2:Room2", "type": "Room2"},
        {"id": "urn:ngsi-ld:Room3:Room3", "type": "Room3"},
        {"id": "urn:ngsi-ld:Toilet:Toilet", "type": "Toilet"}
    ],
    "watchedAttributes": ["temperature", "humidity", "brightness"],
    "notification": {
        "attributes": ["temperature", "humidity", "brightness"],
        "format": "normalized",
        "endpoint": {"uri": CALLBACK, "accept": "application/ld+json"}
    },
    "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
}

def setup_subscription():
    try:
        requests.delete(f"{SUBS_URL}/{SUB_ID}", timeout=5)
    except:
        pass
    r = requests.post(SUBS_URL, json=payload, headers={"Content-Type":"application/ld+json"}, timeout=5)
    print("Subscription:", r.status_code, r.text)

def write_to_hbase(entity):
    try:
        eid = entity["id"]
        entity_type = entity["type"].lower()
        table_name = f"{entity_type}_data"
        temperature = entity.get("temperature", {}).get("value")
        humidity = entity.get("humidity", {}).get("value")
        brightness = entity.get("brightness", {}).get("value")

        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        connection = happybase.Connection(host=HBASE_HOST)
        try:
            connection.create_table(
                table_name,
                {'cf': dict()}  # column family 'cf'
            )
            print(f"✅ Created table: {table_name}")
        except Exception as e:
            if "TableExistsException" in str(e) or "already in use" in str(e):
                print(f"⚠️ Table already exists: {table_name}")
            else:
                print(f"⚠️ Table creation failed: {e}")

        table = connection.table(table_name.encode())

        rowkey = f"{eid}_{ts}".encode()
        data_dict = {}

        if temperature is not None:
            data_dict[b'cf:temperature'] = str(temperature).encode()
        if humidity is not None:
            data_dict[b'cf:humidity'] = str(humidity).encode()
        if brightness is not None:
            data_dict[b'cf:brightness'] = str(brightness).encode()
        data_dict[b'cf:timestamp'] = ts.encode()

        table.put(rowkey, data_dict)
        connection.close()
        print("✅ Inserted into HBase")

    except Exception as e:
        print(f"❌ HBase insert failed: {e}")

def hbase_writer(worker_id):
    while True:
        entity = write_buffer.get()
        print(f"🔧 [Worker {worker_id}] Processing entity: {entity.get('id')}")
        write_to_hbase(entity)
        write_buffer.task_done()

@app.route("/notify", methods=["POST"])
def notify():
    data = request.get_json(force=True)
    print("📥 Notification received:", json.dumps(data, indent=2))

    # Estraggo i dati
    try:
        current_time = time.time()
        for entity in data["data"]:
            eid = entity["id"]
            if current_time - last_sent[eid] >= delay_seconds:
                write_buffer.put(entity)
                last_sent[eid] = current_time
            else:
                print(f"⏳ Skipping {eid} to respect 4s interval")

    except Exception as e:
        print(f"❌ HBase insert failed: {e}")

    return jsonify({"status": "received"}), 200

if __name__ == "__main__":
    # Clean up all relevant tables if they exist before starting
    try:
        connection = happybase.Connection(host=HBASE_HOST)
        for table_name in [b'kitchen_data', b'room1_data', b'room2_data', b'room3_data', b'bathroom_data', b'toilet_data']:
            if table_name in connection.tables():
                connection.delete_table(table_name, disable=True)
                print(f"🧹 Deleted existing table: {table_name.decode()}")
        connection.close()
    except Exception as e:
        print(f"❌ Failed to clean up existing table: {e}")

    writer_threads = []
    for i in range(3):  # Start 3 worker threads
        t = threading.Thread(target=hbase_writer, args=(i,), daemon=True)
        t.start()
        writer_threads.append(t)

    setup_subscription()
    app.run(host="0.0.0.0", port=8000)