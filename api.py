from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json

app = FastAPI(title="API Telemática - TCC")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "telematica"

class EventoTelemetrico(BaseModel):
    driver_id: int
    timestamp: str
    speed: float
    acceleration: float
    harsh_brake: int
    distance: float
    is_night: int


@app.get("/")
def home():
    return {"message": "API com Kafka 🚀"}


@app.post("/event")
def receber_evento(evento: EventoTelemetrico):

    producer.send(TOPIC, evento.dict())
    producer.flush()

    return {"status": "enviado para fila (Kafka)"}