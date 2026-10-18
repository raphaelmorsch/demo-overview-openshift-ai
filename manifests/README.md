Os YAMLs descrevem a demo. Antes de aplicar em outro cluster:

- troque os placeholders `<AISTOR_ACCESS_KEY>` e `<AISTOR_SECRET_KEY>`
- confirme a storage class `gp3-csi` e o domínio de rotas
- não reaplique o gateway de inferência por cima do objeto vivo sem conferir os outros namespaces já permitidos

O ConfigMap `fraude-serve` é criado a partir de `08-model-serving/serve.py`:

```bash
oc create configmap fraude-serve -n repositorio --from-file=serve.py=manifests/08-model-serving/serve.py
```

O ConfigMap `fraude-spark-job` junta o script e as 8 transações originais:

```bash
oc create configmap fraude-spark-job -n repositorio \
  --from-file=fraude_resumo.py=manifests/03-spark/fraude_resumo.py \
  --from-file=transacoes.csv=manifests/03-spark/transacoes-originais.csv
```
