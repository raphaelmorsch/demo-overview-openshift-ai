"""Contrato Feast do projeto fraude.

Aplicado com feast apply no FeatureStore fraude. O parquet offline fica no
PVC feast-fraude-data, montado em /feast-data pelo pod do Feast.
"""

from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float64, Int64, String

transacao = Entity(name="transacao", join_keys=["id"])
lote = Entity(name="lote", join_keys=["lote_id"])

transacao_source = FileSource(
    name="transacoes_spark",
    path="/feast-data/transacoes.parquet",
    timestamp_field="event_timestamp",
)
resumo_source = FileSource(
    name="resumo_spark",
    path="/feast-data/resumo_fraude.parquet",
    timestamp_field="event_timestamp",
)

transacao_features = FeatureView(
    name="transacao_features",
    entities=[transacao],
    ttl=timedelta(days=365),
    schema=[
        Field(name="produto", dtype=String),
        Field(name="valor", dtype=Float64),
        Field(name="fraude", dtype=Int64),
    ],
    source=transacao_source,
    tags={"origem": "spark", "job": "resumo-fraude"},
)

resumo_fraude = FeatureView(
    name="resumo_fraude",
    entities=[lote],
    ttl=timedelta(days=365),
    schema=[
        Field(name="transacoes", dtype=Int64),
        Field(name="valor_total", dtype=Float64),
        Field(name="fraudes", dtype=Int64),
    ],
    source=resumo_source,
    tags={"origem": "spark", "job": "resumo-fraude"},
)

deteccao_fraude = FeatureService(
    name="deteccao_fraude",
    features=[transacao_features],
)
