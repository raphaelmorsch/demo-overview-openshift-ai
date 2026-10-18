#!/usr/bin/env python3
"""Servidor do classificador de fraude. Carrega o joblib publicado em s3://modelos/fraude."""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import joblib
import pandas as pd


def caminho_modelo():
    candidatos = [
        os.environ.get("MODEL_PATH"),
        "/mnt/models/model.joblib",
        "/mnt/models/fraude/model.joblib",
    ]
    for caminho in candidatos:
        if caminho and os.path.exists(caminho):
            return caminho
    raise FileNotFoundError("model.joblib")


MODELO = joblib.load(caminho_modelo())


class Handler(BaseHTTPRequestHandler):
    def _json(self, codigo, corpo):
        dados = json.dumps(corpo).encode()
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def do_GET(self):
        if self.path.startswith("/v1/models") or self.path.startswith("/v2/models") or self.path in ("/health", "/healthz", "/ready"):
            self._json(200, {"name": "fraude-classifier", "ready": True})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        tamanho = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(tamanho) or b"{}")
        instancias = payload.get("instances") or payload.get("inputs")
        if instancias is None and "produto" in payload:
            instancias = [payload]
        tabela = pd.DataFrame(instancias)[["produto", "valor"]]
        tabela["valor"] = tabela["valor"].astype(float)
        probabilidade = MODELO.predict_proba(tabela)[:, 1]
        previsoes = [
            {"fraude": int(valor >= 0.5), "probabilidade": round(float(valor), 4)}
            for valor in probabilidade
        ]
        self._json(200, {"predictions": previsoes})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
