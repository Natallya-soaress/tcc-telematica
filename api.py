from fastapi import FastAPI
from kafka import KafkaProducer
import json
from config import KAFKA_SERVER, KAFKA_TOPIC

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.post("/event")
def receber_evento(evento: dict):
    producer.send(KAFKA_TOPIC, evento)
    producer.flush()
    return {"status": "ok"}