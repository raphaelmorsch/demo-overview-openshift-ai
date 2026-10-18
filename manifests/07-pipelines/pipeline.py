"""Pipeline Fraude: dados, treino e deployment.

Compilado com kfp 2.16 e executado na imagem de Data Science, que já traz
kfp 2.17, scikit-learn, pandas e boto3. install_kfp_package=False evita o
pip install de kfp==2.16.0, versão que não existe no PyPI.
"""

from kfp import compiler, dsl, kubernetes

IMAGEM = (
    "registry.redhat.io/rhoai/odh-workbench-jupyter-datascience-cpu-py312-rhel9"
    "@sha256:206018cabeb3389fcfb554cb1850c82e509483a2f98bf963d82031303adaaee5"
)
SEGREDO = {
    "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
    "AWS_S3_ENDPOINT": "AWS_S3_ENDPOINT",
    "AWS_DEFAULT_REGION": "AWS_DEFAULT_REGION",
}


@dsl.component(base_image=IMAGEM, install_kfp_package=False)
def validar_features(contrato: dsl.Output[dsl.Artifact]):
    import json
    import ssl
    import urllib.request

    token = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read()
    url = "https://feast-fraude-registry-rest.repositorio.svc.cluster.local/api/v1/features?project=fraude"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, context=ssl._create_unverified_context(), timeout=30) as resp:
        corpo = json.load(resp)
    nomes = set()
    for feature in corpo.get("features", corpo if isinstance(corpo, list) else []):
        view = feature.get("featureView") or feature.get("feature_view") or ""
        nome = feature.get("name")
        if view == "transacao_features" and nome:
            nomes.add(nome)
    faltando = {"produto", "valor", "fraude"} - nomes
    if faltando:
        raise RuntimeError(f"features ausentes em transacao_features: {sorted(faltando)}")
    with open(contrato.path, "w") as saida:
        json.dump({"projeto": "fraude", "view": "transacao_features", "features": sorted(nomes)}, saida)


@dsl.component(base_image=IMAGEM, install_kfp_package=False)
def preparar_dados(dados: dsl.Output[dsl.Dataset]):
    import os

    import boto3
    import pandas as pd

    cliente = boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )
    cliente.download_file("dados", "fraude/treino.csv", dados.path)
    tabela = pd.read_csv(dados.path)
    if len(tabela) < 100:
        raise RuntimeError(f"treino.csv tem {len(tabela)} linhas; o minimo e 100")


@dsl.component(base_image=IMAGEM, install_kfp_package=False)
def treinar(
    dados: dsl.Input[dsl.Dataset],
    contrato: dsl.Input[dsl.Artifact],
    modelo: dsl.Output[dsl.Model],
    teste: dsl.Output[dsl.Dataset],
):
    import json

    import joblib
    import pandas as pd
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    with open(contrato.path) as arquivo:
        features = set(json.load(arquivo)["features"])
    if not {"produto", "valor", "fraude"} <= features:
        raise RuntimeError("contrato de features incompleto")

    tabela = pd.read_csv(dados.path)
    treino, holdout = train_test_split(
        tabela, test_size=0.25, random_state=7, stratify=tabela["fraude"]
    )
    pipeline = Pipeline(
        [
            (
                "prep",
                ColumnTransformer(
                    [
                        ("produto", OneHotEncoder(handle_unknown="ignore"), ["produto"]),
                        ("valor", StandardScaler(), ["valor"]),
                    ]
                ),
            ),
            ("clf", LogisticRegression(max_iter=500)),
        ]
    )
    pipeline.fit(treino[["produto", "valor"]], treino["fraude"])
    joblib.dump(pipeline, modelo.path)
    holdout.to_csv(teste.path, index=False)
    modelo.metadata["framework"] = "scikit-learn"
    modelo.metadata["features"] = "produto,valor"


@dsl.component(base_image=IMAGEM, install_kfp_package=False)
def testar(
    modelo: dsl.Input[dsl.Model],
    teste: dsl.Input[dsl.Dataset],
    resultado: dsl.Output[dsl.Dataset],
):
    import joblib
    import pandas as pd

    pipeline = joblib.load(modelo.path)
    tabela = pd.read_csv(teste.path)
    probabilidade = pipeline.predict_proba(tabela[["produto", "valor"]])[:, 1]
    tabela["probabilidade"] = probabilidade.round(4)
    tabela["previsto"] = (tabela["probabilidade"] >= 0.5).astype(int)
    tabela.to_csv(resultado.path, index=False)


@dsl.component(base_image=IMAGEM, install_kfp_package=False)
def avaliar(resultado: dsl.Input[dsl.Dataset], metricas: dsl.Output[dsl.Artifact]):
    import json

    import pandas as pd
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

    tabela = pd.read_csv(resultado.path)
    resumo = {
        "acuracia": round(float(accuracy_score(tabela["fraude"], tabela["previsto"])), 4),
        "precisao": round(float(precision_score(tabela["fraude"], tabela["previsto"])), 4),
        "recall": round(float(recall_score(tabela["fraude"], tabela["previsto"])), 4),
        "f1": round(float(f1_score(tabela["fraude"], tabela["previsto"])), 4),
        "amostras_teste": int(len(tabela)),
    }
    for nome, valor in resumo.items():
        metricas.log_metric(nome, valor) if hasattr(metricas, "log_metric") else None
    if resumo["f1"] < 0.8:
        raise RuntimeError(f"f1 {resumo['f1']} abaixo de 0.8")
    with open(metricas.path, "w") as saida:
        json.dump(resumo, saida)


@dsl.component(base_image=IMAGEM, install_kfp_package=False)
def publicar(modelo: dsl.Input[dsl.Model], metricas: dsl.Input[dsl.Artifact]) -> str:
    import os

    import boto3

    cliente = boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )
    cliente.upload_file(modelo.path, "modelos", "fraude/model.joblib")
    cliente.upload_file(metricas.path, "modelos", "fraude/metricas.json")
    return "s3://modelos/fraude"


@dsl.component(base_image=IMAGEM, install_kfp_package=False)
def implantar(uri: str, job_id: str):
    import json
    import os
    import ssl
    import urllib.error
    import urllib.request

    token = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read()
    namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    base = f"https://{host}"
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    contexto = ssl._create_unverified_context()

    def pedir(metodo, caminho, corpo=None):
        data = json.dumps(corpo).encode() if corpo is not None else None
        req = urllib.request.Request(base + caminho, data=data, headers=headers, method=metodo)
        with urllib.request.urlopen(req, context=contexto, timeout=30) as resp:
            bruto = resp.read()
            return json.loads(bruto) if bruto else {}

    caminho = f"/apis/serving.kserve.io/v1beta1/namespaces/{namespace}/inferenceservices/fraude-classifier"
    manifesto = {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "name": "fraude-classifier",
            "namespace": namespace,
            "labels": {"opendatahub.io/dashboard": "true"},
            "annotations": {
                "openshift.io/display-name": "Classificador de fraude",
                "openshift.io/description": "Classificador tabular de fraude publicado pelo pipeline.",
                "opendatahub.io/hardware-profile-name": "default-profile",
                "opendatahub.io/hardware-profile-namespace": "redhat-ods-applications",
                "security.opendatahub.io/enable-auth": "false",
                "serving.kserve.io/deploymentMode": "Standard",
                "serving.kserve.io/storageSecretName": "fraude-s3",
            },
        },
        "spec": {
            "predictor": {
                "minReplicas": 1,
                "maxReplicas": 1,
                "model": {
                    "modelFormat": {"name": "sklearn", "version": "1"},
                    "runtime": "fraude-sklearn",
                    "protocolVersion": "v1",
                    "storageUri": uri,
                    "resources": {
                        "requests": {"cpu": "200m", "memory": "1Gi"},
                        "limits": {"cpu": "1", "memory": "4Gi"},
                    },
                    "env": [{"name": "MODEL_VERSION", "value": job_id}],
                },
            }
        },
    }
    try:
        atual = pedir("GET", caminho)
        manifesto["metadata"]["resourceVersion"] = atual["metadata"]["resourceVersion"]
        pedir("PUT", caminho, manifesto)
    except urllib.error.HTTPError as erro:
        if erro.code != 404:
            raise
        pedir("POST", f"/apis/serving.kserve.io/v1beta1/namespaces/{namespace}/inferenceservices", manifesto)

    rota = f"/apis/gateway.networking.k8s.io/v1/namespaces/{namespace}/httproutes/fraude-classifier-route"
    try:
        pedir("GET", rota)
    except urllib.error.HTTPError as erro:
        if erro.code != 404:
            raise
        pedir(
            "POST",
            f"/apis/gateway.networking.k8s.io/v1/namespaces/{namespace}/httproutes",
            {
                "apiVersion": "gateway.networking.k8s.io/v1",
                "kind": "HTTPRoute",
                "metadata": {"name": "fraude-classifier-route", "namespace": namespace},
                "spec": {
                    "parentRefs": [
                        {
                            "name": "openshift-ai-inference",
                            "namespace": "openshift-ingress",
                            "sectionName": "https",
                        }
                    ],
                    "hostnames": ["inference-gateway.apps.ocp.p9cjv.sandbox2112.opentlc.com"],
                    "rules": [
                        {
                            "matches": [
                                {
                                    "path": {"type": "PathPrefix", "value": "/"},
                                    "headers": [
                                        {"name": "X-Model", "type": "Exact", "value": "fraude-classifier"}
                                    ],
                                }
                            ],
                            "backendRefs": [{"name": "fraude-classifier-predictor", "port": 80}],
                        }
                    ],
                },
            },
        )


@dsl.pipeline(name="fraude-dados-treino-e-deployment", display_name="Fraude: dados, treino e deployment")
def pipeline():
    features = validar_features()
    features.set_display_name("Validar features")
    dados = preparar_dados()
    dados.set_display_name("Preparar dados")
    modelo = treinar(dados=dados.output, contrato=features.output)
    modelo.set_display_name("Treinar")
    testes = testar(modelo=modelo.outputs["modelo"], teste=modelo.outputs["teste"])
    testes.set_display_name("Testar")
    notas = avaliar(resultado=testes.output)
    notas.set_display_name("Avaliar")
    destino = publicar(modelo=modelo.outputs["modelo"], metricas=notas.output)
    destino.set_display_name("Publicar no S3")
    destino.after(notas)
    deploy = implantar(uri=destino.output, job_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER)
    deploy.set_display_name("Implantar")

    for tarefa in (features, dados, modelo, testes, notas, destino, deploy):
        tarefa.set_caching_options(False)
        kubernetes.set_cpu_request(tarefa, "250m")
        kubernetes.set_cpu_limit(tarefa, "1")
        kubernetes.set_memory_request(tarefa, "2Gi")
        kubernetes.set_memory_limit(tarefa, "4Gi")
    for tarefa in (dados, destino):
        kubernetes.use_secret_as_env(tarefa, secret_name="dados", secret_key_to_env=SEGREDO)


if __name__ == "__main__":
    compiler.Compiler().compile(pipeline, package_path="fraude-pipeline.yaml")
