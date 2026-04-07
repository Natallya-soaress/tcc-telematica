import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib

from feature_engineering import gerar_features, criar_score

df = pd.read_csv("dados_telematicos.csv")

features = gerar_features(df)
features = criar_score(features)

X = features[
    ["freq_frenagem", "velocidade_media", "pct_noite", "aceleracao_media"]
]

y = features["score"]

model = Pipeline([
    ("scaler", StandardScaler()),
    ("regressor", LinearRegression())
])

model.fit(X, y)

joblib.dump(model, "modelo_score.pkl")

print("Modelo treinado com normalização!")