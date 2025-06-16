from pyhive import hive
import time
import csv
import statistics

# Connessione a HiveServer2
conn = hive.Connection(host='localhost', port=10000, username='hive', database='default')
cursor = conn.cursor()


# Stampa della versione di Hive
cursor.execute("SET -v")
version_info = cursor.fetchall()
print("🧠 Hive Configuration (including version):")
for row in version_info:
    if 'hive.execution.version' in row[0].lower() or 'version' in row[0].lower():
        print(row[0])


cursor.execute("DROP TABLE IF EXISTS kitchen_data")
cursor.execute("""
CREATE EXTERNAL TABLE kitchen_data (
    entityid STRING,
    temperature DOUBLE,
    humidity INT,
    brightness DOUBLE,
    ts TIMESTAMP
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:temperature,cf:humidity,cf:brightness,cf:timestamp"
)
TBLPROPERTIES ("hbase.table.name" = "kitchen_data")
""")


cursor.execute("DROP TABLE IF EXISTS room3_data")
cursor.execute("""
CREATE EXTERNAL TABLE room3_data (
    entityid STRING,
    temperature DOUBLE,
    humidity INT,
    brightness DOUBLE,
    ts TIMESTAMP
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:temperature,cf:humidity,cf:brightness,cf:timestamp"
)
TBLPROPERTIES ("hbase.table.name" = "room3_data")
""")


cursor.execute("DROP TABLE IF EXISTS toilet_data")
cursor.execute("""
CREATE EXTERNAL TABLE toilet_data (
    entityid STRING,
    temperature DOUBLE,
    humidity INT,
    brightness DOUBLE,
    ts TIMESTAMP
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:temperature,cf:humidity,cf:brightness,cf:timestamp"
)
TBLPROPERTIES ("hbase.table.name" = "toilet_data")
""")

cursor.execute("DROP TABLE IF EXISTS bathroom_data")
cursor.execute("""
CREATE EXTERNAL TABLE bathroom_data (
    entityid STRING,
    temperature DOUBLE,
    humidity INT,
    brightness DOUBLE,
    ts TIMESTAMP
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:temperature,cf:humidity,cf:brightness,cf:timestamp"
)
TBLPROPERTIES ("hbase.table.name" = "bathroom_data")
""")

# Drop e creazione forzata della tabella Hive
cursor.execute("DROP TABLE IF EXISTS room1_data")

cursor.execute("""
CREATE EXTERNAL TABLE room1_data (
    entityid STRING,
    temperature DOUBLE,
    humidity INT,
    brightness DOUBLE,
    ts TIMESTAMP
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:temperature,cf:humidity,cf:brightness,cf:timestamp"
)
TBLPROPERTIES ("hbase.table.name" = "room1_data")
""")


# Drop e creazione forzata della tabella Hive
cursor.execute("DROP TABLE IF EXISTS room2_data")

cursor.execute("""
CREATE EXTERNAL TABLE room2_data (
    entityid STRING,
    temperature DOUBLE,
    humidity INT,
    brightness DOUBLE,
    ts TIMESTAMP
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:temperature,cf:humidity,cf:brightness,cf:timestamp"
)
TBLPROPERTIES ("hbase.table.name" = "room2_data")
""")

print("✅ Tabella (ri)creata forzatamente.")

queries = [
    ("25_1.txt", """
        SELECT temperature, ts
        FROM kitchen_data
        WHERE unix_timestamp(ts) >= unix_timestamp() - 3600
    """),
    ("25_2.txt", """
        SELECT hour(ts) as hour_bucket, AVG(CAST(temperature AS DOUBLE)) as avg_temp
        FROM kitchen_data
        GROUP BY hour(ts)
        ORDER BY hour_bucket
    """),
    ("25_3.txt", """
        SELECT r1.hour, r1.avg_temp as room1_temp, r2.avg_temp as room2_temp
        FROM (
            SELECT hour(ts) as hour, AVG(CAST(temperature AS DOUBLE)) as avg_temp
            FROM room1_data
            GROUP BY hour(ts)
        ) r1
        JOIN (
            SELECT hour(ts) as hour, AVG(CAST(temperature AS DOUBLE)) as avg_temp
            FROM room2_data
            GROUP BY hour(ts)
        ) r2
        ON r1.hour = r2.hour
    """),
    ("25_4.txt", """
SELECT 
  tab.room,
  hour(tab.ts) AS hour,
  AVG(CAST(tab.temperature AS DOUBLE)) AS avg_temperature,
  MIN(CAST(tab.humidity AS INT)) AS min_humidity,
  MAX(CAST(tab.brightness AS DOUBLE)) AS max_brightness,
  COUNT(*) AS count_readings,
  'room+hour' AS grouping_level
FROM (
  SELECT 'kitchen' AS room, entityid, temperature, humidity, brightness, ts FROM kitchen_data
  UNION ALL
  SELECT 'room1', entityid, temperature, humidity, brightness, ts FROM room1_data
  UNION ALL
  SELECT 'room2', entityid, temperature, humidity, brightness, ts FROM room2_data
  UNION ALL
  SELECT 'room3', entityid, temperature, humidity, brightness, ts FROM room3_data
  UNION ALL
  SELECT 'bathroom', entityid, temperature, humidity, brightness, ts FROM bathroom_data
  UNION ALL
  SELECT 'toilet', entityid, temperature, humidity, brightness, ts FROM toilet_data
) tab
GROUP BY tab.room, hour(tab.ts)

UNION ALL

SELECT 
  tab.room,
  NULL AS hour,
  AVG(CAST(tab.temperature AS DOUBLE)) AS avg_temperature,
  MIN(CAST(tab.humidity AS INT)) AS min_humidity,
  MAX(CAST(tab.brightness AS DOUBLE)) AS max_brightness,
  COUNT(*) AS count_readings,
  'room' AS grouping_level
FROM (
  SELECT 'kitchen' AS room, entityid, temperature, humidity, brightness, ts FROM kitchen_data
  UNION ALL
  SELECT 'room1', entityid, temperature, humidity, brightness, ts FROM room1_data
  UNION ALL
  SELECT 'room2', entityid, temperature, humidity, brightness, ts FROM room2_data
  UNION ALL
  SELECT 'room3', entityid, temperature, humidity, brightness, ts FROM room3_data
  UNION ALL
  SELECT 'bathroom', entityid, temperature, humidity, brightness, ts FROM bathroom_data
  UNION ALL
  SELECT 'toilet', entityid, temperature, humidity, brightness, ts FROM toilet_data
) tab
GROUP BY tab.room

UNION ALL

SELECT 
  NULL AS room,
  hour(tab.ts) AS hour,
  AVG(CAST(tab.temperature AS DOUBLE)) AS avg_temperature,
  MIN(CAST(tab.humidity AS INT)) AS min_humidity,
  MAX(CAST(tab.brightness AS DOUBLE)) AS max_brightness,
  COUNT(*) AS count_readings,
  'hour' AS grouping_level
FROM (
  SELECT 'kitchen' AS room, entityid, temperature, humidity, brightness, ts FROM kitchen_data
  UNION ALL
  SELECT 'room1', entityid, temperature, humidity, brightness, ts FROM room1_data
  UNION ALL
  SELECT 'room2', entityid, temperature, humidity, brightness, ts FROM room2_data
  UNION ALL
  SELECT 'room3', entityid, temperature, humidity, brightness, ts FROM room3_data
  UNION ALL
  SELECT 'bathroom', entityid, temperature, humidity, brightness, ts FROM bathroom_data
  UNION ALL
  SELECT 'toilet', entityid, temperature, humidity, brightness, ts FROM toilet_data
) tab
GROUP BY hour(tab.ts)

UNION ALL

SELECT 
  NULL AS room,
  NULL AS hour,
  AVG(CAST(tab.temperature AS DOUBLE)) AS avg_temperature,
  MIN(CAST(tab.humidity AS INT)) AS min_humidity,
  MAX(CAST(tab.brightness AS DOUBLE)) AS max_brightness,
  COUNT(*) AS count_readings,
  'total' AS grouping_level
FROM (
  SELECT 'kitchen' AS room, entityid, temperature, humidity, brightness, ts FROM kitchen_data
  UNION ALL
  SELECT 'room1', entityid, temperature, humidity, brightness, ts FROM room1_data
  UNION ALL
  SELECT 'room2', entityid, temperature, humidity, brightness, ts FROM room2_data
  UNION ALL
  SELECT 'room3', entityid, temperature, humidity, brightness, ts FROM room3_data
  UNION ALL
  SELECT 'bathroom', entityid, temperature, humidity, brightness, ts FROM bathroom_data
  UNION ALL
  SELECT 'toilet', entityid, temperature, humidity, brightness, ts FROM toilet_data
) tab
""")
]

for filename, sql in queries:
    print(f"Running query from {filename}...")
    times = []
    warmup = time.time()
    cursor.execute(sql)
    cursor.fetchall()
    warmup_duration = time.time() - warmup
    times.append(warmup_duration)

    for _ in range(30):
        start = time.time()
        cursor.execute(sql)
        cursor.fetchall()
        end = time.time()
        times.append(end - start)

    mean = statistics.mean(times[1:])
    stdev = statistics.stdev(times[1:])
    conf = 1.96 * stdev / (len(times[1:]) ** 0.5)

    with open(filename, "w") as f:
        f.write(f"Warm-up: {times[0]:.6f} sec\n")
        for i, t in enumerate(times[1:], 1):
            f.write(f"Run {i}: {t:.6f} sec\n")
        f.write(f"\nMedia: {mean:.6f} sec\n")
        f.write(f"Deviazione standard: {stdev:.6f} sec\n")
        f.write(f"Intervallo 95%: ±{conf:.6f} sec\n")
    print(f"Query from {filename} executed. Results saved to {filename}.")