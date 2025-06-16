import os
import glob
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

ORION_LD_URL = "http://localhost:1026/ngsi-ld/v1/entities/"
HEADERS = {"Content-Type": "application/ld+json"}
DELAY = 0.25
MAX_PERCENT_PER_THREAD = 0.25 
last_room = None
update_counts = {}

def create_entity_if_absent(room, context):
    entity_id = f"urn:ngsi-ld:{room}:{room}"
    context_attr = context.lower()
    try:
        r = requests.get(f"{ORION_LD_URL}{entity_id}", headers={"Accept": "application/ld+json"}, timeout=5)
        if r.status_code == 404:
            entity_payload = {
                "id": entity_id,
                "type": room,
                context_attr: {
                    "type": "Property",
                    "value": None
                },
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            }
            r_create = requests.post(ORION_LD_URL, headers=HEADERS, json=entity_payload, timeout=5)
            r_create.raise_for_status()
            print(f"[{room}] Pre-creata entità per attributo '{context}'")
    except Exception as e:
        print(f"[{room}] Errore nella pre-creazione entità: {e}")

def send_patch(room, context, timestamp, value):
    print(f"➡️ CHIAMATA PATCH: {room}.{context} = {value} @ {timestamp.isoformat()}")
    entity_id = f"urn:ngsi-ld:{room}:{room}"
    patch_url = f"{ORION_LD_URL}{entity_id}/attrs"
    context_attr = context.lower()

    attr_payload = {
    context_attr: {
        "type": "Property",
        "value": float(value),
        "observedAt": timestamp.isoformat()
    },
        "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"}

    try:
        r = requests.patch(patch_url, headers=HEADERS, json=attr_payload, timeout=5)
        print(f"⬅️ STATUS PATCH: {r.status_code}")
        print(f"⬅️ BODY PATCH: {r.text}")
        r.raise_for_status()
        data = {}
        if r.content:
            try:
                data = r.json()
            except ValueError as e:
                print(f"[{room}] ⚠️ Errore decoding JSON: {e} - Body: {r.text}")

        if "notUpdated" in data and any(attr.get("attributeName") == context_attr for attr in data["notUpdated"]):
            print(f"[{room}] ⚠️ Attributo '{context_attr}' non presente. Lo creo ora.")
            entity_payload = {
                "id": entity_id,
                "type": room,
                context_attr: {
                    "type": "Property",
                    "value": float(value),
                    "observedAt": timestamp.isoformat()
                },
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            }
            try:
                r_append = requests.post(patch_url, headers=HEADERS, json={
                    context_attr: {
                        "type": "Property",
                        "value": float(value),
                        "observedAt": timestamp.isoformat()
                    },
                    "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
                }, timeout=5)
                r_append.raise_for_status()
                print(f"[{room}] ✅ Attributo '{context_attr}' creato via PATCH")
            except requests.exceptions.HTTPError as e2:
                print(f"[{room}] Errore creazione attributo con PATCH: {e2}")
        else:
            print(f"[{room}] PATCH {context_attr} = {value} @ {timestamp.isoformat()}")
            print(f"🔗 Visualizza: {ORION_LD_URL}{entity_id}?options=keyValues")
            update_counts[room] = update_counts.get(room, 0) + 1
            print(f"[{room}] 🔢 Contatore aggiornamenti (in memoria): {update_counts[room]}")

    except requests.exceptions.HTTPError as e:
        print(f"[{room}] Errore HTTP PATCH: {e} (status: {r.status_code})")
        print(f"→ Risposta Orion: {r.text}")
        if r.status_code in (404, 400):
            entity_payload = {
                "id": entity_id,
                "type": room,
                context_attr: attr_payload[context_attr],
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            }
            try:
                r_create = requests.post(ORION_LD_URL, headers=HEADERS, json=entity_payload, timeout=5)
                r_create.raise_for_status()
                print(f"[{room}] Entity created with '{context_attr}' = {value}")
            except requests.exceptions.HTTPError as e2:
                if r_create.status_code == 409:
                    print(f"[{room}] Entity già presente (409)")
                else:
                    print(f"[{room}] Errore creazione entità: {e2}")
        else:
            print(f"[{room}/{context}] Errore PATCH @ {timestamp}: {e}")

def simulate_file_stream(file_path):
    global last_room
    filename = os.path.basename(file_path)
    room, context = filename.split("_")[0], filename.split("_", 1)[1].rsplit(".", 1)[0]
    context_attr = context.lower()

    print(f"📄 FILE: {filename} ➜ ROOM: {room}, ATTR: {context_attr}")

    if last_room == room:
        time.sleep(1)
    last_room = room

    with open(file_path, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        max_lines = int(len(lines) * MAX_PERCENT_PER_THREAD)
        if max_lines == 0:
            print(f"[{filename}] ⚠️ Nessuna riga da inviare (MAX_PERCENT_PER_THREAD troppo basso)")
            return
        lines = lines[:max_lines]

        for line_number, line in enumerate(lines, start=1):
            parts = line.split("\t")
            if len(parts) != 2:
                print(f"[{filename}] Riga {line_number} malformata: '{line}' (attesi 2 campi)")
                continue

            ts_raw, val_raw = parts
            try:
                ts = datetime.now(timezone.utc)
                print(f"[DEBUG] Usato timestamp attuale: {ts.isoformat()}")
                val = float(val_raw)
                send_patch(room, context, ts, val)
                time.sleep(DELAY)
            except Exception as e:
                print(f"[{filename}] Errore parsing riga {line_number}: {e}")

    table_name = f"{room.lower()}_data"
    count_rows_in_hbase(table_name, room)

def simulate_all(folder="./Measurements"):
    files = glob.glob(os.path.join(folder, "*.csv"))
    from collections import defaultdict
    room_files = defaultdict(list)
    for file in files:
        room = os.path.basename(file).split("_")[0]
        room_files[room].append(file)

    existing_attrs = set()

    for file in files:
        filename = os.path.basename(file)
        room = filename.split("_")[0]
        context = filename.replace(".csv", "").split("_", 1)[1]
        key = (room, context.lower())
        if key not in existing_attrs:
            create_entity_if_absent(room, context)
            existing_attrs.add(key)

    def simulate_room_stream(room, file_list):
        open_files = []
        for file_path in file_list:
            with open(file_path, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
                max_lines = int(len(lines) * MAX_PERCENT_PER_THREAD)
                open_files.append(iter(lines[:max_lines]))

        more_data = True
        while more_data:
            more_data = False
            for i, line_iter in enumerate(open_files):
                try:
                    line = next(line_iter)
                    context = os.path.basename(file_list[i]).split("_", 1)[1].replace(".csv", "")
                    parts = line.split("\t")
                    if len(parts) != 2:
                        print(f"[{room}] Riga malformata: '{line}'")
                        continue
                    ts = datetime.now(timezone.utc)
                    val = float(parts[1])
                    send_patch(room, context, ts, val)
                    time.sleep(1)
                    more_data = True
                except StopIteration:
                    continue

    # Assign rooms to threads
    thread_rooms = [
        ["Kitchen", "Room1"],
        ["Room2", "Room3"],
        ["Bathroom", "Toilet"]
    ]

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for room_group in thread_rooms:
            files_subset = {room: room_files[room] for room in room_group if room in room_files}
            for room, file_list in files_subset.items():
                futures.append(executor.submit(simulate_room_stream, room, file_list))
        for future in futures:
            future.result()

if __name__ == "__main__":
    simulate_all()