"""Gera o CSV de treino com 800 linhas.

As 8 primeiras linhas são as transações originais do Spark. As outras 792 são
geradas com a semente 42. Só as linhas geradas podem ter o rótulo invertido
(4%). O arquivo da demo foi enviado para s3://dados/fraude/treino.csv.
"""

import csv
import random
from pathlib import Path

ORIGINAIS = [
    (1001, "cartao", 250.40, 0),
    (1002, "cartao", 8900.00, 1),
    (1003, "pix", 40.00, 0),
    (1004, "cartao", 120.10, 0),
    (1005, "pix", 15000.00, 1),
    (1006, "cartao", 75.90, 0),
    (1007, "cartao", 4300.00, 1),
    (1008, "pix", 18.50, 0),
]


def main():
    random.seed(42)
    destino = Path(__file__).resolve().parents[1] / "notebooks" / "treino.csv"
    with destino.open("w", newline="") as arquivo:
        writer = csv.writer(arquivo)
        writer.writerow(["id", "produto", "valor", "fraude"])
        for linha in ORIGINAIS:
            writer.writerow(linha)
        for identificador in range(2000, 2792):
            produto = random.choice(["cartao", "pix"])
            valor = round(10 ** random.uniform(1, 4.3), 2)
            limite = 2500 if produto == "pix" else 4000
            fraude = 1 if valor >= limite else 0
            if random.random() < 0.04:
                fraude = 1 - fraude
            writer.writerow([identificador, produto, valor, fraude])
    print(f"escreveu {destino} com 800 linhas")


if __name__ == "__main__":
    main()
