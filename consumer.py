import json
import time
import csv
import os

from kafka import KafkaConsumer
import tenseal as ts
import psutil

from config import USE_HE, KAFKA_TOPIC, KAFKA_SERVER
from he_utils import context, decrypt_vector
from db import salvar_resultado

CSV_FILE = "metricas_he.csv" if USE_HE else "metricas_plain.csv"

print(f"Consumer iniciado | HE={'ON' if USE_HE else 'OFF'}")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest"
)

WEIGHTS = [0.3, 0.2, 0.25, 0.15, 0.1]
BIAS = 0.5

inicio_global = time.perf_counter()
eventos_processados = 0


def calcular_score_plain(valores):
    return sum(v * w for v, w in zip(valores, WEIGHTS)) + BIAS


def calcular_score_he(enc_vector):
    result = enc_vector.dot(WEIGHTS)
    result += BIAS
    return result


def inicializar_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "cenario",
                "volume",
                "carga",
                "execucao",
                "evento",
                "latencia_total",
                "tempo_processamento",
                "tempo_cifragem",
                "tempo_decifragem",
                "throughput",
                "cpu_percent",
                "mem_percent",
                "tamanho_payload"
            ])


def salvar_metricas(linha):
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(linha)


inicializar_csv()

for msg in consumer:
    evento = msg.value

    start_total = time.perf_counter()

    driver_id = evento.get("driver_id")
    timestamp = evento.get("timestamp")

    tempo_cifragem = evento.get("tempo_cifragem", 0)
    tempo_dec = 0
    tamanho_payload = 0

    cpu_before = psutil.cpu_percent()
    mem_before = psutil.virtual_memory().percent

    try:
        if USE_HE:
            payload_hex = evento["payload_encrypted"]
            payload_bytes = bytes.fromhex(payload_hex)

            tamanho_payload = len(payload_bytes)

            enc_vector = ts.ckks_vector_from(context, payload_bytes)

            start_proc = time.perf_counter()
            enc_result = calcular_score_he(enc_vector)
            end_proc = time.perf_counter()

            score, tempo_dec = decrypt_vector(enc_result)
            score = score[0]

        else:
            valores = [
                evento["speed"],
                evento["acceleration"],
                evento["harsh_brake"],
                evento["distance"],
                evento["is_night"]
            ]

            start_proc = time.perf_counter()
            score = calcular_score_plain(valores)
            end_proc = time.perf_counter()

        salvar_resultado(driver_id, score, timestamp)

        end_total = time.perf_counter()

        eventos_processados += 1

        tempo_decorrido = end_total - inicio_global
        throughput = eventos_processados / tempo_decorrido if tempo_decorrido > 0 else 0

        cpu_after = psutil.cpu_percent()
        mem_after = psutil.virtual_memory().percent

        salvar_metricas([
            "HE" if USE_HE else "PLAIN",
            evento.get("teste_volume"),
            evento.get("teste_carga"),
            evento.get("teste_execucao"),
            evento.get("teste_evento"),
            end_total - start_total,
            end_proc - start_proc,
            tempo_cifragem,
            tempo_dec,
            throughput,
            cpu_after,
            mem_after,
            tamanho_payload
        ])

        print(
            f"Teste | HE={USE_HE} | "
            f"Volume={evento.get('teste_volume')} | "
            f"Carga={evento.get('teste_carga')} | "
            f"Execução={evento.get('teste_execucao')} | "
            f"Evento={evento.get('teste_evento')}\n"
            f"Score={score:.4f}\n"
            f"Latência={end_total-start_total:.6f}s\n"
            f"Tempo processamento={end_proc-start_proc:.6f}s\n"
            f"Throughput={throughput:.2f} eventos/s\n"
            f"CPU={cpu_after:.2f}% | Memória={mem_after:.2f}%"
        )

        if USE_HE:
            print(
                f"Tempo cifragem={tempo_cifragem:.6f}s\n"
                f"Tempo decifragem={tempo_dec:.6f}s\n"
                f"Payload cifrado={tamanho_payload} bytes"
            )

        print("-" * 60)

    except Exception as e:
        print(f"Erro ao processar evento: {e}")