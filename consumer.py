from kafka import KafkaConsumer
import json
import pandas as pd
import joblib
from collections import defaultdict

from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import sessionmaker, declarative_base

consumer = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",  
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tcc-group-ml",  
)

consumer.subscribe(["telematica"])
print("Consumidor rodando...\n")


DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/telematica"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class EventoDB(Base):
    __tablename__ = "eventos"

    id = Column(Integer, primary_key=True)
    driver_id = Column(Integer)
    timestamp = Column(String)
    speed = Column(Float)
    acceleration = Column(Float)
    harsh_brake = Column(Integer)
    distance = Column(Float)
    is_night = Column(Integer)
    score = Column(Float)


Base.metadata.create_all(bind=engine)

try:
    model = joblib.load("modelo_score.pkl")
except Exception as e:
    print("Erro ao carregar modelo:", e)
    exit()

historico = defaultdict(list)

for msg in consumer:

    try:
        evento = json.loads(msg.value.decode("utf-8"))
    except Exception as e:
        print("Erro ao decodificar JSON:", e)
        continue

    driver_id = evento.get("driver_id")

    if driver_id is None:
        print("Evento sem driver_id, ignorando")
        continue

    historico[driver_id].append(evento)

    df = pd.DataFrame(historico[driver_id])

    if len(df) < 10:
        continue

    features = {
        "freq_frenagem": df["harsh_brake"].mean(),
        "velocidade_media": df["speed"].mean(),
        "pct_noite": df["is_night"].mean(),
        "aceleracao_media": df["acceleration"].mean()
    }

    X = pd.DataFrame([features])

    try:
        score = model.predict(X)[0]
        score = round(max(0, min(100, float(score))), 2)
    except Exception as e:
        print("Erro ao calcular score:", e)
        continue

    try:
        db = SessionLocal()

        novo = EventoDB(
            driver_id=evento["driver_id"],
            timestamp=evento["timestamp"],
            speed=evento["speed"],
            acceleration=evento["acceleration"],
            harsh_brake=evento["harsh_brake"],
            distance=evento["distance"],
            is_night=evento["is_night"],
            score=score
        )

        db.add(novo)
        db.commit()
        db.close()

    except Exception as e:
        print("Erro ao salvar no banco:", e)