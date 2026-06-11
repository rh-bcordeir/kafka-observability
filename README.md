# Observações

## Kafka Cluster

Rota chamada de "listener" no yaml do cluster usa TLS. 

- Para conectar ao cluster é necessário truststore configurado. Pode ser com p12 ou jks
- Usuário com permissão para o tópico usa TLS. Para isso é necessário mTLS. Por isso para conectar é necessário configurar o truststore do kafka user

## Kafka Console

Se definir usuário dentro do yaml fica com opção de logar de forma anonima

## Configuração de segurança (conexão com o Kafka)

O cluster (Strimzi) expõe três listeners, cada um com um tipo de autenticação diferente
(ver [`infra/1-cluster.yaml`](infra/1-cluster.yaml)):

| Listener     | Tipo       | Porta | TLS    | Autenticação    | Acesso          |
|--------------|------------|-------|--------|-----------------|-----------------|
| `plain`      | `internal` | 9092  | não    | `scram-sha-512` | só dentro do cluster (`.svc`) |
| `tls`        | `internal` | 9093  | sim    | `scram-sha-512` | só dentro do cluster (`.svc`) |
| `listener`   | `route`    | 9094  | sim    | `tls` (mTLS)    | externo (rota)  |
| `extscram`   | `route`    | 9095  | sim    | `scram-sha-512` | externo (rota)  |

> **Interno x externo:** os listeners `internal` só resolvem pelo DNS interno
> (`kafka-cluster-kafka-bootstrap.kafka.svc:<porta>`), ou seja, só funcionam para apps
> rodando **dentro** do cluster. Os listeners do tipo `route` são expostos pelo router do
> OpenShift e acessíveis de **fora** — mas atenção: a conexão externa é sempre na **porta
> 443** (rota TLS-passthrough), **não** na porta do listener (9094/9095). Pegue o endereço
> real com:
>
> ```shell script
> oc get kafka kafka-cluster -n kafka \
>   -o jsonpath='{range .status.listeners[?(@.name=="extscram")]}{.bootstrapServers}{"\n"}{end}'
> ```

> **Quando usar `extscram`:** é o único listener que permite **SCRAM de fora do cluster**.
> O `listener` (mTLS) também é externo, mas exige certificado de cliente; os listeners
> SCRAM `plain`/`tls` só são alcançáveis internamente. Para rodar o producer no laptop
> autenticando com usuário/senha SCRAM, conecte no `extscram` (`SASL_SSL` + truststore da
> CA do cluster, sem keystore).

A forma de configurar as `properties` do producer **depende do tipo de autenticação do
`KafkaUser`** que você está usando. O tipo de autenticação do usuário precisa bater com o
tipo de autenticação do listener ao qual você conecta.

> As properties abaixo usam o prefixo do canal SmallRye (`mp.messaging.outgoing.orders.*`).
> Tudo que vem depois do prefixo é uma propriedade nativa do cliente Kafka.

### 1. KafkaUser do tipo `tls` (mTLS)

Aqui a identidade do cliente **é o próprio certificado**. O Strimzi gera um Secret para o
usuário contendo `user.p12` (keystore) e a senha. Não existe usuário/senha — quem prova
quem você é, é a chave privada dentro do keystore.

```properties
mp.messaging.outgoing.orders.security.protocol=SSL

# Truststore: valida o certificado do broker (lado servidor)
mp.messaging.outgoing.orders.ssl.truststore.location=/caminho/ca.p12
mp.messaging.outgoing.orders.ssl.truststore.password=${CA_PASSWORD}
mp.messaging.outgoing.orders.ssl.truststore.type=PKCS12

# Keystore: certificado do usuário (lado cliente / sua identidade)
mp.messaging.outgoing.orders.ssl.keystore.location=/caminho/kafka-user-producer.p12
mp.messaging.outgoing.orders.ssl.keystore.password=${USER_PASSWORD}
mp.messaging.outgoing.orders.ssl.key.password=${USER_PASSWORD}
mp.messaging.outgoing.orders.ssl.keystore.type=PKCS12
```

Pontos-chave do mTLS:

- `security.protocol=SSL` (apenas TLS, sem camada SASL).
- Precisa de **truststore** (validar o broker) **e** **keystore** (apresentar o seu certificado).
- É o que o listener `listener` (porta 9094) exige.

### 2. KafkaUser do tipo `scram-sha-512`

Aqui a identidade é **usuário + senha** (SCRAM). O Strimzi gera um Secret com a senha do
usuário. **Não há keystore**, porque você não se autentica com certificado.

```properties
# SASL_SSL  -> SCRAM por cima de uma conexão TLS
#   - interno: listener "tls", porta 9093 (.svc)
#   - externo: listener "extscram", rota na porta 443
# SASL_PLAINTEXT -> SCRAM sem TLS (listener "plain", porta 9092, só interno)
mp.messaging.outgoing.orders.security.protocol=SASL_SSL
mp.messaging.outgoing.orders.sasl.mechanism=SCRAM-SHA-512
mp.messaging.outgoing.orders.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="user-producer" password="${KAFKA_USER_PASSWORD}";

# Truststore: necessário SOMENTE quando há TLS (SASL_SSL).
# Com SASL_PLAINTEXT (porta 9092) estas três linhas não são usadas.
mp.messaging.outgoing.orders.ssl.truststore.location=/caminho/ca.p12
mp.messaging.outgoing.orders.ssl.truststore.password=${CA_PASSWORD}
mp.messaging.outgoing.orders.ssl.truststore.type=PKCS12
```

Pontos-chave do SCRAM:

- `security.protocol=SASL_SSL` (com TLS) ou `SASL_PLAINTEXT` (sem TLS).
- `sasl.mechanism=SCRAM-SHA-512`.
- A identidade vai no `sasl.jaas.config` (usuário e senha) — **não** existe keystore/`ssl.key.password`.

### Resumo das diferenças

| Property                | `tls` (mTLS)        | `scram-sha-512`                 |
|-------------------------|---------------------|---------------------------------|
| `security.protocol`     | `SSL`               | `SASL_SSL` ou `SASL_PLAINTEXT`  |
| `sasl.mechanism`        | —                   | `SCRAM-SHA-512`                 |
| `sasl.jaas.config`      | —                   | usuário + senha (ScramLoginModule) |
| `ssl.truststore.*`      | obrigatório         | só com TLS (`SASL_SSL`)         |
| `ssl.keystore.*`        | obrigatório         | não usa                         |
| `ssl.key.password`      | obrigatório         | não usa                         |

## Por que configurar o truststore do cluster?

O truststore guarda a **CA do cluster Kafka**. Ele serve para o cliente **validar o
certificado que o broker apresenta** durante o handshake TLS — ou seja, garantir que você
está realmente falando com o broker certo e que a conexão é criptografada.

O Strimzi assina os certificados dos brokers com uma **CA própria, auto-assinada**
(`<cluster>-cluster-ca-cert`). Essa CA **não está** no `cacerts` padrão da JVM, então, sem
importá-la no truststore, o handshake TLS falha com erro de certificado não confiável.

Repare que o truststore é sempre sobre a identidade do **servidor (broker)**, e é
independente da forma como o **cliente** se autentica:

- Com **mTLS** você precisa de truststore (validar o broker) **e** keystore (sua identidade).
- Com **SCRAM sobre TLS** (`SASL_SSL`) você precisa de truststore, mas **não** de keystore.

### Quando o truststore NÃO é necessário

- **Listener sem TLS** (`plain`, porta 9092 → `SASL_PLAINTEXT`): não há handshake TLS, logo
  não há certificado de broker para validar. O tráfego, porém, **não é criptografado** —
  use apenas em ambiente confiável/interno.
- **Broker com certificado assinado por uma CA pública/conhecida** que já esteja no
  `cacerts` padrão da JVM. Nesse caso a CA já é confiável por padrão e não é preciso um
  truststore próprio. Não é o caso da CA auto-assinada gerada pelo Strimzi.