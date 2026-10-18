from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("resumo-fraude").getOrCreate()
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("/mnt/spark/data/transacoes.csv")
    .repartition(8)
)
df.cache()
df.count()

resumo = (
    df.groupBy("produto", "fraude")
    .agg(
        F.count("*").alias("transacoes"),
        F.round(F.sum("valor"), 2).alias("valor_total"),
        F.round(F.avg("valor"), 2).alias("ticket_medio"),
    )
    .orderBy("produto", "fraude")
)
resumo.show(truncate=False)

janela = Window.partitionBy("produto").orderBy(F.desc("valor"))
(
    df.withColumn("posicao", F.row_number().over(janela))
    .filter(F.col("posicao") <= 5)
    .select("id", "produto", "valor", "fraude", "posicao")
    .orderBy("produto", "posicao")
    .show(truncate=False)
)

totais = df.agg(F.sum("valor").alias("valor"), F.count("*").alias("n")).collect()[0]
fraudes = df.filter(F.col("fraude") == 1).count()
print(
    f"RESUMO_FRAUDE transacoes={totais['n']} valor_total={totais['valor']:.2f} fraudes={fraudes}"
)
spark.stop()
