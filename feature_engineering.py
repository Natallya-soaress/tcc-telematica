import pandas as pd

def gerar_features(df):
    return {
        "freq_frenagem": df["harsh_brake"].mean(),
        "velocidade_media": df["speed"].mean(),
        "pct_noite": df["is_night"].mean(),
        "aceleracao_media": df["acceleration"].mean()
    }