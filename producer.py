import random
from datetime import datetime
import requests
import time

from config import USE_HE
from he_utils import encrypt_vector

URL = "http://127.0.0.1:8000/event"

VOLUMES = [100, 500, 1000, 2000, 5000]

CARGAS = {
    "baixa": 0.1,
    "media": 0.05,
    "alta": 0.01
}

EXECUCOES = 3


def gerar_evento():
    return {
        "driver_id": random.randint(1, 50),
        "timestamp": datetime.now().isoformat(),
        "speed": random.uniform(40, 120),
        "acceleration": random.uniform(-5, 3),
        "harsh_brake": random.randint(0, 1),
        "distance": random.uniform(0.5, 2),
        "is_night": random.randint(0, 1),
    }


def enviar(evento):
    try:
        response = requests.post(URL, json=evento, timeout=120)

        if response.status_code != 200:
            print(f"Erro HTTP {response.status_code}")

    except Exception as e:
        print(f"Erro ao enviar: {e}")


def preparar_evento(volume, carga, execucao, indice):
    evento = gerar_evento()

    evento["teste_volume"] = volume
    evento["teste_carga"] = carga
    evento["teste_execucao"] = execucao
    evento["teste_evento"] = indice

    if USE_HE:
        valores = [
            evento["speed"],
            evento["acceleration"],
            evento["harsh_brake"],
            evento["distance"],
            evento["is_night"]
        ]

        enc, tempo = encrypt_vector(valores)

        evento["payload_encrypted"] = enc.serialize().hex()
        evento["tempo_cifragem"] = tempo

        del evento["speed"]
        del evento["acceleration"]
        del evento["harsh_brake"]
        del evento["distance"]
        del evento["is_night"]

    return evento


def executar_teste(volume, carga, intervalo, execucao):
    print(
        f"Teste | HE={USE_HE} | Volume={volume} | "
        f"Carga={carga} | Execução={execucao}"
    )

    inicio = time.perf_counter()

    for i in range(1, volume + 1):
        evento = preparar_evento(volume, carga, execucao, i)
        enviar(evento)
        time.sleep(intervalo)

    fim = time.perf_counter()

    print(f"Duração total: {fim - inicio:.4f}s")


if __name__ == "__main__":
    for volume in VOLUMES:
        for carga, intervalo in CARGAS.items():
            for execucao in range(1, EXECUCOES + 1):
                executar_teste(volume, carga, intervalo, execucao)
                time.sleep(5)