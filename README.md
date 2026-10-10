# Проект обработки заказов

## Кратко о проекте

Проект реализует потоковую обработку событий заказов через Kafka и PostgreSQL. Сервисы разделены на три слоя:

- `solution/service_stg` принимает сообщения о заказах из Kafka и сохраняет исходные события в staging-слое PostgreSQL.
- `solution/service_dds` обрабатывает события из STG, формирует сущности, связи и историю атрибутов в Data Vault.
- `solution/service_cdm` читает поток DDS и обновляет витрины счётчиков заказов по пользователям, товарам и категориям.

Каждый сервис — отдельное приложение с конфигурацией из переменных окружения, Kafka-коннекторами, PostgreSQL-репозиторием и обработчиком сообщений. `solution/docker-compose.yaml` описывает локальный запуск сервисов.


### Поток данных и контракты

Проверяйте изменения с учётом направления потока: **Kafka → STG → DDS → CDM**. При изменении структуры события проверьте все места, которые читают или публикуют это событие: processor/job, repository, примеры JSON и SQL. Обратите внимание на соответствие имён и форматов полей между слоями.

В DDS пакетная загрузка (`load_batch`) выполняет несколько отдельных INSERT в рамках одного соединения/транзакции.

DDS и CDM используют PostgreSQL `uuid_generate_v5` для детерминированных идентификаторов. Namespace передаётся через `NAMESPACE_UUID`: конфигурация сервиса читает переменную окружения, а приложение передаёт значение репозиторию. 


### Тестирование

Unit-тесты используют mock PostgreSQL/Kafka-зависимостей и лежат в `tests/` соответствующих сервисов. Они подтверждают контракт вызовов и формируемые параметры.

## Для изучения сообщений в Kafka

```bash
export MSYS_NO_PATHCONV=1
docker run -it --name kcat --network=host --rm \
  -v 'C:/Users/алексей/.kafka/YandexInternalRootCA.crt:/data/CA.pem' \
  edenhill/kcat:1.7.1 \
  -b rc1a-02p8am8as40u1qrb.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username=producer_consumer \
  -X sasl.password="****" \
  -X ssl.ca.location=/data/CA.pem \
  -t stg-service-orders -C -o beginning
```
