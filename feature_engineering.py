import pandas as pd

def gerar_features(df):

    features = df.groupby("driver_id").agg({
        "harsh_brake": "mean",
        "speed": "mean",
        "is_night": "mean",
        "acceleration": "mean"
    }).reset_index()

    features.rename(columns={
        "harsh_brake": "freq_frenagem",
        "speed": "velocidade_media",
        "is_night": "pct_noite",
        "acceleration": "aceleracao_media"
    }, inplace=True)

    return features


def criar_score(df):

    score = (
        100
        - df["freq_frenagem"] * 40
        - (df["velocidade_media"] - 70).clip(lower=0) * 0.3
        - df["pct_noite"] * 15
        - df["aceleracao_media"].abs() * 8
    )

    df["score"] = score.clip(0, 100)

    return df