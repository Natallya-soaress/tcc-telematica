import random
from datetime import datetime, timedelta
import csv
import requests
import time
import sys


PERFIS = ["conservador", "moderado", "agressivo"]


def gerar_evento(driver_id, perfil, tempo):

    if perfil == "conservador":
        speed = random.uniform(40, 80)
        acceleration = random.uniform(-2, 1.5)

    elif perfil == "moderado":
        speed = random.uniform(50, 100)
        acceleration = random.uniform(-3, 2)

    else:
        speed = random.uniform(70, 140)
        acceleration = random.uniform(-5, 3)

    if acceleration < -3:
        harsh_brake = 1
    else:
        if perfil == "conservador":
            harsh_brake = 1 if random.random() < 0.03 else 0
        elif perfil == "moderado":
            harsh_brake = 1 if random.random() < 0.08 else 0
        else:
            harsh_brake = 1 if random.random() < 0.20 else 0

    distance = speed / 60

    if perfil == "conservador":
        is_night = 1 if random.random() < 0.2 else 0
    elif perfil == "moderado":
        is_night = 1 if random.random() < 0.35 else 0
    else:
        is_night = 1 if random.random() < 0.5 else 0

    speed = round(speed, 1)
    acceleration = round(acceleration, 2)
    distance = round(distance, 2)

    return {
        "driver_id": driver_id,
        "timestamp": tempo.isoformat(),
        "speed": speed,
        "acceleration": acceleration,
        "harsh_brake": harsh_brake,
        "distance": distance,
        "is_night": is_night,
        "perfil": perfil
    }

def gerar_dados(qtd=1000):

    dados = []
    tempo = datetime.now()

    for _ in range(qtd):
        tempo += timedelta(seconds=random.randint(1, 5))

        driver_id = random.randint(1, 50)
        perfil = random.choice(PERFIS)

        evento = gerar_evento(driver_id, perfil, tempo)
        dados.append(evento)

    return dados


def salvar_csv(dados, nome_arquivo="dados_telematicos.csv"):

    if not dados:
        return

    with open(nome_arquivo, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=dados[0].keys())
        writer.writeheader()
        writer.writerows(dados)

    print(f"CSV salvo: {nome_arquivo}")


def enviar_evento_api(evento):
    url = "http://localhost:8000/event"

    evento_envio = evento.copy()
    evento_envio.pop("perfil", None)

    try:
        response = requests.post(url, json=evento_envio)
        print(f"Enviado: {response.status_code}")
    except Exception as e:
        print(f"Erro ao enviar: {e}")


if __name__ == "__main__":

    modo = input("Escolha o modo (1 = CSV treino | 2 = Enviar API): ")

    if modo == "1":
        qtd = int(input("Quantidade de eventos para CSV: "))
        dados = gerar_dados(qtd)
        salvar_csv(dados)

    elif modo == "2":
        total_eventos = int(input("Quantos eventos enviar? "))

        print(f"Iniciando envio de {total_eventos} eventos...")

        tempo_atual = datetime.now()

        for i in range(total_eventos):
            tempo_atual += timedelta(seconds=random.randint(1, 5))

            driver_id = random.randint(1, 50)
            perfil = random.choice(PERFIS)

            evento = gerar_evento(driver_id, perfil, tempo_atual)
            enviar_evento_api(evento)

            if (i + 1) % 10 == 0:
                print(f"{i + 1}/{total_eventos} enviados")

            time.sleep(0.2)

        print("Envio concluído!")

    else:
        print("Modo inválido.")