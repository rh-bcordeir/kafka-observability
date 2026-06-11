# Monitoramento do cluster Kafka no Grafana

Este guia descreve como habilitar o monitoramento de um cluster Kafka (operado pelo [Strimzi](https://strimzi.io/)) e visualizar as métricas no Grafana, usando o stack de monitoramento do OpenShift (Prometheus + Thanos Querier) como fonte de dados.

Todos os manifestos referenciados estão nesta pasta (`metrics/`).

## Visão geral do fluxo

```
Kafka (JMX Exporter) ──┐
Kafka Exporter ────────┼──> PodMonitor ──> Prometheus (User Workload) ──> Thanos Querier ──> Grafana
                       │
ScrapeMetrics ─────────┘
```

1. O cluster Kafka expõe métricas via **JMX Prometheus Exporter** e **Kafka Exporter**.
2. Um **PodMonitor** instrui o Prometheus a coletar (scrape) essas métricas.
3. O Prometheus de *user workload* armazena as métricas e as disponibiliza através do **Thanos Querier**.
4. O **Grafana** consome o Thanos Querier como datasource e exibe os **dashboards**.

## Pré-requisitos

- Cluster OpenShift com o operador **Strimzi** instalado.
- Cluster Kafka implantado no namespace `kafka`.
- CLI `oc` autenticada com permissões de administrador.
- Um namespace `grafana` para os componentes do Grafana (`oc new-project grafana`).

---

## 1. Expor as métricas do Kafka (JMX Exporter)

Configure o cluster Kafka para exportar métricas via JMX Prometheus Exporter e crie o `ConfigMap` com as regras de conversão das métricas.

O arquivo [kafka-metrics.yaml](kafka-metrics.yaml) contém:

- Os `KafkaNodePool` (controller/broker) e o recurso `Kafka` (`my-cluster`), já com a seção `metricsConfig` apontando para o ConfigMap:

  ```yaml
  metricsConfig:
    type: jmxPrometheusExporter
    valueFrom:
      configMapKeyRef:
        name: kafka-metrics
        key: kafka-metrics-config.yml
  ```

- O `ConfigMap` **kafka-metrics** com as regras (`kafka-metrics-config.yml`) do JMX exporter.

Aplique:

```bash
oc apply -f kafka-metrics.yaml -n kafka
```

> Caso o cluster Kafka já exista, basta garantir que a seção `metricsConfig` e o `ConfigMap kafka-metrics` estejam presentes.

---

## 2. Adicionar o Kafka Exporter ao cluster

O **Kafka Exporter** expõe métricas adicionais de consumer groups, lag e offsets de tópicos.

O arquivo [kafka-exporter.yaml](kafka-exporter.yaml) mostra a seção `kafkaExporter` que deve ser adicionada ao `spec` do recurso `Kafka`. Use-o como referência para complementar o `Kafka` definido em [kafka-metrics.yaml](kafka-metrics.yaml) (que já traz uma configuração mínima de `kafkaExporter`).

Pontos principais:

```yaml
spec:
  kafkaExporter:
    topicRegex: ".*"
    groupRegex: ".*"
```

Após editar, reaplique o recurso Kafka:

```bash
oc apply -f kafka-metrics.yaml -n kafka
```

---

## 3. Criar o PodMonitor

O **PodMonitor** instrui o Prometheus a coletar as métricas dos pods do Strimzi (Kafka, Kafka Connect e MirrorMaker2).

Aplique o arquivo [kafka-resources-metrics.yaml](kafka-resources-metrics.yaml):

```bash
oc apply -f kafka-resources-metrics.yaml -n kafka
```

Esse manifesto define um `PodMonitor` chamado `kafka-resources-metrics` que:

- Seleciona pods pelo label `strimzi.io/kind` (valores `Kafka`, `KafkaConnect`, `KafkaMirrorMaker2`).
- Coleta no endpoint `/metrics`, porta `tcp-prometheus`.
- Aplica `relabelings` para enriquecer as métricas com `namespace`, `pod`, `node` e `node_ip`.

---

## 4. Criar as Prometheus Rules

Aplique o arquivo [prometheus-rules.yaml](prometheus-rules.yaml):

```bash
oc apply -f prometheus-rules.yaml -n kafka
```

Esse manifesto define um `PrometheusRule` chamado `strimzi-kube-state-metrics` com regras de alerta baseadas nas métricas do Strimzi, cobrindo recursos como `Kafka`, `KafkaTopic`, `KafkaUser`, `KafkaNodePool`, `KafkaRebalance`, `KafkaConnect(or)`, `KafkaMirrorMaker2` e `KafkaAccess` — alertando, por exemplo, quando um recurso não fica `Ready` (`KafkaNotReady`, `KafkaTopicNotReady`, ...) ou usa configuração depreciada (`*Deprecated`).

---

## 5. Habilitar o User Workload Monitoring

O Prometheus padrão do OpenShift não coleta métricas de workloads de usuário. Habilite essa funcionalidade editando (ou criando) o `ConfigMap` `cluster-monitoring-config` no namespace `openshift-monitoring`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-monitoring-config
  namespace: openshift-monitoring
data:
  config.yaml: |
    enableUserWorkload: true
```

```bash
oc apply -f cluster-monitoring-config.yaml
```

Isso provisiona o stack de monitoramento de user workload que vai coletar as métricas definidas pelo PodMonitor do passo 3.

---

## 6. Criar a Service Account do Grafana e dar permissão

O Grafana precisa de uma `ServiceAccount` com permissão de leitura nas métricas do cluster.

Crie a Service Account no namespace `grafana`:

```bash
oc create sa grafana-service-account -n grafana
```

Em seguida, conceda a permissão `cluster-monitoring-view` aplicando o [grafana-cluster-monitoring-binding.yaml](grafana-cluster-monitoring-binding.yaml):

```bash
oc apply -f grafana-cluster-monitoring-binding.yaml
```

Esse `ClusterRoleBinding` associa a `ServiceAccount grafana-service-account` (namespace `grafana`) ao `ClusterRole cluster-monitoring-view`.

---

## 7. Criar a Secret com o token da Service Account

O datasource do Grafana se autentica no Thanos Querier usando um token Bearer da Service Account. Crie uma `Secret` do tipo token com a annotation que dispara a geração automática do token:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: grafana-sa-token
  namespace: grafana
  annotations:
    kubernetes.io/service-account.name: grafana-service-account
type: kubernetes.io/service-account-token
```

```bash
oc apply -f grafana-sa-token.yaml -n grafana
```

Recupere o token (será usado no próximo passo):

```bash
oc get secret grafana-sa-token -n grafana -o jsonpath='{.data.token}' | base64 -d
```

---

## 8. Configurar o datasource do Grafana (Thanos Querier)

O Grafana consome as métricas através do **Thanos Querier** do `openshift-monitoring`.

O arquivo [datasource.yaml](datasource.yaml) define o datasource Prometheus apontando para:

```
https://thanos-querier.openshift-monitoring.svc.cluster.local:9091
```

A autenticação é feita via header `Authorization: Bearer ${GRAFANA-ACCESS-TOKEN}` — substitua pelo token obtido no passo 7.

Crie o `ConfigMap` a partir do arquivo:

```bash
oc create configmap grafana-config --from-file=datasource.yaml -n grafana
```

Esse ConfigMap deve ser montado no deployment do Grafana (passo 9).

---

## 9. Implantar o Grafana (Deployment, Service e Route)

Crie o `Deployment` e o `Service` do Grafana no namespace `grafana`, montando o `ConfigMap grafana-config` (datasource) criado no passo anterior.

Em seguida, exponha o Grafana externamente com uma `Route`:

```bash
oc expose service grafana -n grafana
oc get route grafana -n grafana
```

Acesse a URL retornada pela Route para abrir o Grafana.

---

## 10. Importar os dashboards

Os dashboards prontos do Strimzi estão na pasta [dashboards/](dashboards/):

- [strimzi-kafka.json](dashboards/strimzi-kafka.json) — métricas gerais do cluster Kafka.
- [strimzi-kraft.json](dashboards/strimzi-kraft.json) — métricas específicas do modo KRaft.

No Grafana: **Dashboards → New → Import**, faça o upload do arquivo JSON e selecione o datasource **Prometheus** configurado no passo 8.

---

## Resumo dos arquivos

| Arquivo | Recurso | Função |
| --- | --- | --- |
| [kafka-metrics.yaml](kafka-metrics.yaml) | `Kafka`, `KafkaNodePool`, `ConfigMap` | Cluster Kafka + JMX Exporter (regras de métricas) |
| [kafka-exporter.yaml](kafka-exporter.yaml) | trecho `spec.kafkaExporter` | Habilita o Kafka Exporter (lag/offsets) |
| [kafka-resources-metrics.yaml](kafka-resources-metrics.yaml) | `PodMonitor` | Scrape das métricas dos pods Strimzi |
| [prometheus-rules.yaml](prometheus-rules.yaml) | `PrometheusRule` | Regras de alerta dos recursos Strimzi |
| [grafana-cluster-monitoring-binding.yaml](grafana-cluster-monitoring-binding.yaml) | `ClusterRoleBinding` | Permissão `cluster-monitoring-view` para o Grafana |
| [datasource.yaml](datasource.yaml) | datasource Grafana | Conexão com o Thanos Querier |
| [dashboards/](dashboards/) | JSON | Dashboards do Strimzi para importar |

## Verificação

- Confirme que o PodMonitor está sendo descoberto: as métricas devem aparecer no Prometheus de user workload.
- No Grafana, abra um dashboard e verifique se há dados — caso contrário, valide o token (passo 7) e a URL do Thanos Querier no datasource (passo 8).
