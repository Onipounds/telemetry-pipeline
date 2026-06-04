import os, sys, json, time, random, datetime, csv

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Femio\keys\bq-key.json"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

BOOTSTRAP, PROJECT, DATASET, TABLE, N = "localhost:9092", "telemetry-warehouse", "telemetry", "pipeline_hourly", 500
devices = [f"device-{i}" for i in range(1, 9)]
metrics = {"temperature": (75.0, 4.0), "vibration": (2.0, 0.3), "pressure": (30.0, 1.5)}

# ===== STAGE 1: stream -> Kafka =====
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
topic = f"pipeline-{int(time.time())}"
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result()
print(f"[1/4] stream: topic {topic}; producing {N} readings...")
producer = Producer({"bootstrap.servers": BOOTSTRAP})
base = datetime.datetime(2026, 1, 1)
for i in range(1, N + 1):
    dev, metric = random.choice(devices), random.choice(list(metrics))
    mean, sd = metrics[metric]
    val = random.gauss(mean, sd)
    if random.random() < 0.02:
        val += random.choice([-1, 1]) * sd * random.uniform(6, 12)
    ts = base + datetime.timedelta(seconds=random.randint(0, 7*24*3600))
    producer.produce(topic, key=dev, value=json.dumps(
        {"reading_id": i, "device_id": dev, "metric": metric,
         "value": round(val, 4), "reading_ts": ts.isoformat(sep=" ")}))
    producer.poll(0)
producer.flush()
print(f"      produced {N} messages")

# ===== STAGE 2: consume -> landing CSV =====
consumer = Consumer({"bootstrap.servers": BOOTSTRAP,
                     "group.id": f"lander-{topic}", "auto.offset.reset": "earliest"})
consumer.subscribe([topic])
got, idle = 0, 0
with open("landed.csv", "w", newline="") as f:
    wtr = csv.writer(f); wtr.writerow(["reading_id","device_id","metric","value","reading_ts"])
    while got < N and idle < 10:
        m = consumer.poll(1.0)
        if m is None: idle += 1; continue
        if m.error(): continue
        r = json.loads(m.value())
        wtr.writerow([r["reading_id"], r["device_id"], r["metric"], r["value"], r["reading_ts"]])
        got += 1
consumer.close()
admin.delete_topics([topic])
print(f"[2/4] land: consumed {got} messages -> landed.csv")

# ===== STAGE 3: Spark transform =====
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.master("local[*]").appName("pipeline")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
schema = "reading_id long, device_id string, metric string, value double, reading_ts timestamp"
hourly = (spark.read.csv("landed.csv", header=True, schema=schema)
          .withColumn("hour", F.date_trunc("hour", "reading_ts"))
          .groupBy("device_id", "metric", "hour")
          .agg(F.round(F.avg("value"), 6).alias("avg_value"), F.count("*").alias("n_readings")))
hourly_pd = hourly.toPandas()
spark.stop()
print(f"[3/4] transform: Spark produced {len(hourly_pd)} hourly rows")

# ===== STAGE 4: load -> BigQuery =====
from google.cloud import bigquery
client = bigquery.Client(project=PROJECT, location="EU")
table_id = f"{PROJECT}.{DATASET}.{TABLE}"
client.load_table_from_dataframe(hourly_pd, table_id,
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
print(f"[4/4] warehouse: loaded {client.get_table(table_id).num_rows} rows into {table_id}")
print("PIPELINE COMPLETE: Kafka -> landing -> Spark -> BigQuery")