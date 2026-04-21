from kafka import KafkaConsumer
import json
import pandas as pd
import joblib
from collections import defaultdict

from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import sessionmaker, declarative_base

import time
import psutil
import csv
import os

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
    print("Modelo carregado\n")
except Exception as e:
    print("Erro ao carregar modelo:", e)
    exit()

historico = defaultdict(list)

start_global = time.time()
contador_eventos = 0

latencias = []
tempos_score = []

arquivo_metricas = "metricas.csv"

if not os.path.exists(arquivo_metricas):
    with open(arquivo_metricas, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "eventos",
            "throughput",
            "latencia_media",
            "tempo_score_medio",
            "cpu",
            "memoria"
        ])

for msg in consumer:

    inicio_evento = time.time()
    contador_eventos += 1

    try:
        evento = json.loads(msg.value.decode("utf-8"))
    except Exception as e:
        print("Erro ao decodificar JSON:", e)
        continue

    driver_id = evento.get("driver_id")

    if driver_id is None:
        continue

    historico[driver_id].append(evento)
    df = pd.DataFrame(historico[driver_id])

    if len(df) < 5:
        continue

    features = {
        "freq_frenagem": df["harsh_brake"].mean(),
        "velocidade_media": df["speed"].mean(),
        "pct_noite": df["is_night"].mean(),
        "aceleracao_media": df["acceleration"].mean()
    }

    X = pd.DataFrame([features])

    try:
        inicio_score = time.time()

        score = model.predict(X)[0]

        fim_score = time.time()

        tempo_score = fim_score - inicio_score
        tempos_score.append(tempo_score)

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

    fim_evento = time.time()
    latencia = fim_evento - inicio_evento
    latencias.append(latencia)

    if contador_eventos % 10 == 0:

        tempo_total = time.time() - start_global
        throughput = contador_eventos / tempo_total

        cpu = psutil.cpu_percent()
        memoria = psutil.virtual_memory().percent

        latencia_media = sum(latencias) / len(latencias)
        tempo_score_medio = sum(tempos_score) / len(tempos_score)

        print("\nMÉTRICAS:")
        print(f"Eventos processados: {contador_eventos}")
        print(f"Throughput: {throughput:.2f} eventos/seg")
        print(f"Latência média: {latencia_media:.4f} s")
        print(f"Tempo médio de score: {tempo_score_medio:.4f} s")
        print(f"CPU: {cpu}%")
        print(f"Memória: {memoria}%\n")

        with open(arquivo_metricas, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                contador_eventos,
                round(throughput, 2),
                round(latencia_media, 4),
                round(tempo_score_medio, 4),
                cpu,
                memoria
            ])