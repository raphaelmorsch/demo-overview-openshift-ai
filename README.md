# Demo overview do OpenShift AI

Roteiro de cerca de 30 a 35 minutos no OpenShift AI Self-Managed 3.5. O caminho da demo vai de processamento de dados com Spark, no estilo de uma plataforma de dados, até o modelo publicado num Inference Server. O Llama que já estava no cluster continua no vLLM, usando a única GPU.

Os manifestos em `manifests/` reproduzem o que foi aplicado. Senhas, chaves do AIStor, a licença e o JWT não estão neste repositório. Os objetos de segredo apontam para nomes que existem no cluster.

## Ambiente

| Item | Valor |
| --- | --- |
| OpenShift | 4.20.41, nó único `ip-10-0-12-56.ec2.internal` (`g6.8xlarge`, us-east-1) |
| API | `https://api.ocp.p9cjv.sandbox2112.opentlc.com:6443` |
| Console | https://console-openshift-console.apps.ocp.p9cjv.sandbox2112.opentlc.com |
| OpenShift AI | Self-Managed 3.5.1, `DataScienceCluster` `default-dsc` |
| Dashboard | https://rh-ai.apps.ocp.p9cjv.sandbox2112.opentlc.com |
| Login | IdP `rhbk`, usuário `admin` |
| Projeto da demo | `repositorio`, nome de exibição **Repositório de modelos e dados** |
| Storage class | `gp3-csi` |

Componentes usados: dashboard, workbenches, KServe 0.19, Kubeflow Pipelines 2.16, Feast 0.65, Kubeflow Spark Operator (Spark 4.0.1) e os MCP servers do Gen AI Studio.

## Roteiro

1. **Dashboard e projeto.** Abrir [Repositório de modelos e dados](https://rh-ai.apps.ocp.p9cjv.sandbox2112.opentlc.com/projects/repositorio). Mostrar workbenches, pipelines, model servers, connections e cluster storage.
2. **GPU sharing.** Uma NVIDIA L4 com time-slicing em 4 fatias. O Llama 3.2 3B Instruct ocupa uma fatia no vLLM, com `gpu-memory-utilization=0.55`. Time-slicing reparte tempo de GPU; a memória de vídeo continua inteira nesse modelo. Os hardware profiles `default-profile`, `gpu-profile` e `gpu-workbench` estão em `redhat-ods-applications`.
3. **Storage.** AIStor no namespace `aistor`, com os buckets `dados`, `modelos` e `pipeline-artifacts`. As connections S3 do projeto apontam para `http://aistor-s3.aistor.svc.cluster.local:9000`.
4. **Spark.** Job `resumo-fraude` sobre transações. A interface que permanece depois do job é o [Spark History Server](https://spark-history-repositorio.apps.ocp.p9cjv.sandbox2112.opentlc.com/).
5. **Feature Store.** Projeto Feast `fraude`, view `transacao_features` (`produto`, `valor`, `fraude`) produzida a partir do Spark. UI em [Feature Store](https://rh-ai.apps.ocp.p9cjv.sandbox2112.opentlc.com/develop-train/feature-store/overview).
6. **MCP.** No Gen AI Studio, em AI asset endpoints, os servers `openshift-mcp-server` e `terraform-mcp-server` no namespace `mcp-servers`.
7. **Workbench.** Workbench **Fraude**, notebook `fraude.ipynb`. Lê o contrato no Feature Store, treina e publica em `s3://modelos/fraude`.
8. **Pipeline.** `Fraude: dados, treino e deployment`. O run verde é `fraude-demo`.
9. **Inference Server.** Deployment **Classificador de fraude**, CPU, profile `default-profile`. A chamada pública está no final deste README.

O classificador é tabular (scikit-learn). O vLLM fica no Llama. Subir este modelo em vLLM, ou pedir uma segunda GPU, disputaria a memória da L4.

## 1. GPU sharing

ConfigMap `time-slicing-config` no namespace `nvidia-gpu-operator`, chave `nvidia-l4`:

- `replicas: 4`
- `renameByDefault: false`
- `failRequestsGreaterThanOne: true`
- `migStrategy: none`

O nó passa a anunciar `nvidia.com/gpu: 4`, produto `NVIDIA-L4-SHARED`. O Llama (`llama-32-3b-instruct`, namespace `my-first-model`) pede `nvidia.com/gpu: 1`, com `max-model-len` 8192 e `enforce-eager`. O classificador de fraude e o workbench usam `default-profile` e não pedem GPU.

Manifesto: `manifests/01-gpu/time-slicing-config.yaml`.

## 2. Storage e connections

AIStor FREE, um nó, PVC `aistor-data` de 30Gi.

| Acesso | Endereço |
| --- | --- |
| S3 dentro do cluster | `http://aistor-s3.aistor.svc.cluster.local:9000` (path-style) |
| S3 pela rota | https://aistor-s3.apps.ocp.p9cjv.sandbox2112.opentlc.com |
| Console | https://aistor-console.apps.ocp.p9cjv.sandbox2112.opentlc.com |

Buckets e connections do projeto (tipo S3, label `opendatahub.io/dashboard=true`):

| Secret | Bucket | Conteúdo |
| --- | --- | --- |
| `dados` | `dados` | `fraude/transacoes.csv` (8 linhas do Spark) e `fraude/treino.csv` (800 linhas) |
| `modelos` | `modelos` | `fraude/model.joblib`, `fraude/metricas.json`, `fraude/LEIA-ME.txt` |
| `pipeline-artifacts` | `pipeline-artifacts` | artefatos do Kubeflow Pipelines, prefixo `v2/artifacts` |

O secret `fraude-s3` é a cópia usada pelo KServe, com anotações `serving.kserve.io/s3-*` para endpoint HTTP, path-style e sem verificação TLS. Os YAMLs em `manifests/02-storage/` têm os placeholders `<AISTOR_ACCESS_KEY>` e `<AISTOR_SECRET_KEY>`.

O CSV de treino tem as 8 transações originais do Spark e 792 linhas geradas (`scripts/gerar_treino.py`, semente 42). As originais não têm o rótulo invertido. Nas geradas, 4% dos rótulos são invertidos. A regra de rótulo é fraude quando `valor` passa de 2500 no pix e de 4000 no cartão.

## 3. Spark

Operator no namespace `redhat-ods-applications`. A service account `spark-operator-spark` e a Role `spark-role` estão no projeto `repositorio`. Imagem `quay.io/opendatahub/data-processing:Spark-v4.0.1`.

A primeira execução, só com as 8 linhas do ConfigMap, terminou com:

```text
RESUMO_FRAUDE transacoes=8 valor_total=28704.90 fraudes=3
```

Essa execução não gravou event log. A segunda leitura o CSV ampliado no PVC `spark-history` (as 8 linhas originais mais 80.000 geradas, 80.008 no total), com 2 executors, e gravou o event log. Resultado:

```text
RESUMO_FRAUDE transacoes=80008 valor_total=209299691.06 fraudes=21003
```

Aplicação no History Server: `spark-f074f62e57b149c28fc1e1de8ba53194`, cerca de 23 segundos, 14 jobs. A UI mostra Jobs, Stages, Storage, Environment, Executors e SQL / DataFrame.

Um job novo só aparece nessa UI se usar:

```text
spark.eventLog.enabled=true
spark.eventLog.dir=file:///mnt/spark/events
```

e montar o PVC `spark-history` em `/mnt/spark`. O History Server lê `file:///mnt/spark/events` na porta 18080.

Manifestos: `manifests/03-spark/` e `manifests/09-spark-history/`.

## 4. Feature Store

`FeatureStore` `fraude`, label `feature-store-ui=enabled`, `feastProject: fraude`. O operator sobe o registry, a online store e o offline store em arquivo, nos PVCs `feast-fraude-registry`, `feast-fraude-online` e `feast-fraude-data`.

Contrato aplicado:

| Objeto | Nome | Detalhe |
| --- | --- | --- |
| Entity | `transacao` | join key `id` |
| Entity | `lote` | join key `lote_id` |
| Feature view | `transacao_features` | `produto` (string), `valor` (float64), `fraude` (int64). Tags `origem=spark`, `job=resumo-fraude` |
| Feature view | `resumo_fraude` | totais do lote |
| Feature service | `deteccao_fraude` | expõe `transacao_features` |

O registry REST, de dentro do cluster:

```text
https://feast-fraude-registry-rest.repositorio.svc.cluster.local/api/v1/features?project=fraude
```

O certificado do serviço é autoassinado. O pipeline e o notebook usam TLS sem verificação. O parquet offline fica no PVC do Feast, que é `ReadWriteOnce` e já está montado pelo pod do Feast; o workbench não monta esse volume. O treino usa o CSV do bucket `dados`, que contém as mesmas 8 transações.

Definições: `manifests/04-feature-store/`.

## 5. MCP servers

Namespace `mcp-servers`, com `openshift-mcp-server` e `terraform-mcp-server` em Ready. O catálogo do Gen AI Studio lê o ConfigMap `gen-ai-aa-mcp-servers` em `redhat-ods-applications`.

Na demo: **Gen AI studio → AI asset endpoints → MCP servers**.

## 6. Workbench

Notebook `fraude`, imagem interna `s2i-generic-data-science-notebook:3.5` (commit `47efb6c`, Python 3.12, CPU). Hardware profile `default-profile`. O PVC `fraude` tem 10Gi. As credenciais S3 entram por `envFrom` no secret `dados`.

O notebook está em `notebooks/fraude.ipynb` e também em `/opt/app-root/src/fraude.ipynb` no PVC. Abrir pelo dashboard:

https://rh-ai.apps.ocp.p9cjv.sandbox2112.opentlc.com/notebook/repositorio/fraude/lab

`notebooks.opendatahub.io/inject-auth=true` faz o dashboard tratar a imagem como atual. O sidecar `kube-rbac-proxy` escuta em 8443 e encaminha para o Jupyter em 127.0.0.1:8888. Sem esse sidecar, a rota do gateway fica sem endpoint.

O `NOTEBOOK_ARGS` não pode repetir `--ServerApp.root_dir`. O entrypoint da imagem já define esse argumento, e a duplicata impede o Jupyter de subir.

O Elyra/Kale grava o pipeline server em `/opt/app-root/src/.config/kale/kfp_server_config.json` na subida do workbench.

Manifesto: `manifests/06-workbench/notebook.yaml`.

## 7. Pipeline

`DataSciencePipelinesApplication` `dspa` no projeto, Ready. API server com OAuth, sem pipeline de exemplo, cache desligado. MariaDB interna (`mlpipeline`) e object storage externo no bucket `pipeline-artifacts` do AIStor, HTTP, path-style.

O launcher gerado pelo operator gravava `disableSSL: false` mesmo com storage HTTP. O ConfigMap `kfp-launcher-aistor` corrige isso e está referenciado em `spec.apiServer.customKfpLauncherConfigMap`. O workflow controller também precisou de `artifactRepository.s3.insecure: true` no ConfigMap `ds-pipeline-workflow-controller-dspa`. Um reconcile do operator pode reverter esse segundo ConfigMap, porque o template inverte o flag.

Rota do pipeline server: https://ds-pipeline-dspa-repositorio.apps.ocp.p9cjv.sandbox2112.opentlc.com

Pipeline **Fraude: dados, treino e deployment**. Cada componente usa a imagem de Data Science do workbench com `install_kfp_package=False`. O compilador 2.16 pedia `kfp==2.16.0`, que não está no PyPI, e a imagem já traz `kfp` 2.17.

Etapas, todas com cache desligado:

1. **Validar features.** Consulta o registry do Feast e exige `produto`, `valor` e `fraude` na view `transacao_features`.
2. **Preparar dados.** Baixa `s3://dados/fraude/treino.csv` e falha se houver menos de 100 linhas.
3. **Treinar.** `train_test_split` com `test_size=0.25`, `random_state=7` e `stratify`. Pipeline do scikit-learn: `OneHotEncoder` em `produto`, `StandardScaler` em `valor`, `LogisticRegression(max_iter=500)`.
4. **Testar.** Grava `produto`, `valor`, `fraude`, `probabilidade` e `previsto`.
5. **Avaliar.** Segue só se o F1 for pelo menos 0,8.
6. **Publicar no S3.** Envia `model.joblib` e `metricas.json` para `s3://modelos/fraude/`.
7. **Implantar.** Cria ou atualiza o `InferenceService` `fraude-classifier`. A versão mais nova do pipeline também cria o `HTTPRoute` se ele não existir.

Run para mostrar na demo, já concluído em 3 min 28 s, com os sete nós verdes:

https://rh-ai.apps.ocp.p9cjv.sandbox2112.opentlc.com/develop-train/pipelines/runs/repositorio/runs/b4bce298-f9aa-4686-a095-ea473accbb18

| Identificador | Valor |
| --- | --- |
| Experiment `fraude` | `3c6accba-f947-405c-80c4-b372486b12d6` |
| Pipeline | `a3483c3e-dab1-454a-bcc6-7b766c6b9a96` |
| Versão executada (`sem-reinstalacao`) | `9a936012-d246-4467-a116-3f67ae5391a9` |
| Versão mais nova (`com-rota`, inclui o HTTPRoute) | `c8586732-ee4c-4818-8cef-f243805341ee` |
| Run `fraude-demo` | `b4bce298-f9aa-4686-a095-ea473accbb18` |

Métricas em `s3://modelos/fraude/metricas.json`: acurácia 0,965, precisão 1,0, recall 0,8542, F1 0,9213, 200 amostras de teste.

A versão executada não cria a rota; a rota foi aplicada à parte e já está aceita. Um run novo usa a versão `com-rota` e republica o modelo. Para a demo, o run verde basta.

Código: `manifests/07-pipelines/pipeline.py`. A Role `implantar-fraude` permite ao `pipeline-runner-dspa` e à service account `fraude` criar `InferenceService` e `HTTPRoute`.

## 8. Model serving

`ServingRuntime` `fraude-sklearn`: a mesma imagem de Data Science, comando `python /opt/serve/serve.py`, porta 8080, sem GPU. O script está em `manifests/08-model-serving/serve.py` e no ConfigMap `fraude-serve`.

`InferenceService` `fraude-classifier`:

- modo `Standard`
- `storageUri: s3://modelos/fraude`
- secret `fraude-s3`
- `modelFormat` sklearn v1
- auth desligada (`security.opendatahub.io/enable-auth=false`)
- profile `default-profile`
- 1 réplica, CPU 200m a 1, memória 1Gi a 4Gi

O gateway `openshift-ai-inference` (namespace `openshift-ingress`) teve o namespace `repositorio` incluído no seletor de rotas do listener HTTPS. O `HTTPRoute` `fraude-classifier-route` exige o header `X-Model: fraude-classifier` e encaminha para `fraude-classifier-predictor:80`.

Chamada verificada, HTTP 200:

```bash
curl -sk -X POST \
  https://inference-gateway.apps.ocp.p9cjv.sandbox2112.opentlc.com/v1/models/fraude-classifier:predict \
  -H 'Content-Type: application/json' \
  -H 'X-Model: fraude-classifier' \
  -d '{"instances":[{"produto":"cartao","valor":8900},{"produto":"pix","valor":40}]}'
```

```json
{"predictions":[{"fraude":1,"probabilidade":0.9773},{"fraude":0,"probabilidade":0.0525}]}
```

Essas duas linhas são as transações 1002 e 1003 do Spark.

O Llama continua em `https://inference-gateway.apps.ocp.p9cjv.sandbox2112.opentlc.com/v1/chat/completions` com o header `X-Model: llama-32-3b-instruct`.

## Layout

```text
manifests/00-projeto/          Namespace e anotações do projeto
manifests/01-gpu/              Time-slicing da L4
manifests/02-storage/          Connections S3 e secret do KServe, sem segredos reais
manifests/03-spark/            RBAC, script e SparkApplication
manifests/04-feature-store/    FeatureStore e o contrato Feast
manifests/06-workbench/        Notebook Fraude
manifests/07-pipelines/        DSPA, launcher, RBAC e o pipeline
manifests/08-model-serving/    Runtime, servidor, InferenceService e HTTPRoute
manifests/09-spark-history/    History Server, PVC e CSV ampliado
notebooks/fraude.ipynb         Notebook do workbench
scripts/gerar_treino.py        CSV de 800 linhas usado no treino
```

Não há manifesto do deployment do AIStor nem dos MCP servers: a licença e o catálogo ficaram só no cluster. O Llama também não está aqui; a demo só ajustou `gpu-memory-utilization` para `0.55` e não deve voltar esse valor para `0.95`.
