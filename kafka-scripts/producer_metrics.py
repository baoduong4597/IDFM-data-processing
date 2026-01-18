from kafka import KafkaProducer
import requests, json, time

producer = KafkaProducer(
    bootstrap_servers=['master:9092', 'worker1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

YARN_METRICS_URL = "http://master:8088/ws/v1/cluster/metrics"

while True:
    try:
        metrics = requests.get(YARN_METRICS_URL).json()
        producer.send('hadoop-metrics', metrics)
        producer.flush()
        print("Sent metrics:", metrics)
    except Exception as e:
        print("Error:", e)

    time.sleep(20)
