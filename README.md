# Проект 9 спринта облачные технологии

До падения инфры сохранил себе сообщения на вход staging, поэтому смог реализовать проект в docker compose.
Kafka, valkey and postgres поднял по новой
При необходимости проверить витрины, данные для подключения к бд можно найти чартах

## Кратко о проекте

Проект реализует потоковую обработку событий заказов через Kafka и PostgreSQL. Сервисы разделены на три слоя:

- `solution/service_stg` принимает сообщения о заказах из Kafka и сохраняет исходные события в staging-слое PostgreSQL.
- `solution/service_dds` обрабатывает события из STG, формирует сущности, связи и историю атрибутов в Data Vault.
- `solution/service_cdm` читает поток DDS и обновляет витрины счётчиков заказов по пользователям, товарам и категориям.

Каждый сервис — отдельное приложение с конфигурацией из переменных окружения, Kafka-коннекторами, SQL-репозиторием и обработчиком сообщений. `solution/docker-compose.yaml` описывает локальный запуск сервисов.

В DDS пакетная загрузка (`load_batch`) выполняет несколько отдельных INSERT в рамках одного соединения и транзакции.

DDS и CDM используют PostgreSQL `uuid_generate_v5` для детерминированных идентификаторов. Namespace передаётся через `NAMESPACE_UUID`: конфигурация сервиса читает переменную окружения, а приложение передаёт значение репозиторию. 




## Для изучения сообщений в Kafka

```bash
export MSYS_NO_PATHCONV=1
docker run -it --name kcat --network=host --rm \
  -v 'C:/Users/алексей/.kafka/YandexInternalRootCA.crt:/data/CA.pem' \
  edenhill/kcat:1.7.1 \
  -b rc1d-0l57s6psggnjbckl.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username=producer_consumer \
  -X sasl.password="xs9rNfQwnKUbUk8" \
  -X ssl.ca.location=/data/CA.pem \
  -t order-service-orders -C -o beginning
```
export MSYS_NO_PATHCONV=1
docker run -it --name kcat2 --network=host --rm \
  -v 'C:/Users/алексей/.kafka/YandexInternalRootCA.crt:/data/CA.pem' \
  edenhill/kcat:1.7.1 \
  -b rc1d-0l57s6psggnjbckl.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username=producer_consumer \
  -X sasl.password="xs9rNfQwnKUbUk8" \
  -X ssl.ca.location=/data/CA.pem \
  -t order-service-orders -P