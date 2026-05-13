import time
import psutil
import csv
from collections import deque

latencias = []
tempos_score = []
eventos_processados = 0

cpu_samples = []
mem_samples = []

start_global = time.time()

def registrar_latencia(valor):
    latencias.append(valor)

def registrar_score(valor):
    tempos_score.append(valor)

def coletar_recursos():
    cpu_samples.append(psutil.cpu_percent())
    mem_samples.append(psutil.virtual_memory().percent)

def salvar_metricas():
    total_tempo = time.time() - start_global

    throughput = eventos_processados / total_tempo if total_tempo > 0 else 0

    with open("metricas.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([
            "eventos", "throughput", "latencia_media",
            "tempo_score_medio", "cpu", "memoria"
        ])
        writer.writerow([
            eventos_processados,
            round(throughput, 2),
            round(sum(latencias)/len(latencias), 4) if latencias else 0,
            round(sum(tempos_score)/len(tempos_score), 4) if tempos_score else 0,
            round(sum(cpu_samples)/len(cpu_samples), 2) if cpu_samples else 0,
            round(sum(mem_samples)/len(mem_samples), 2) if mem_samples else 0,
        ])