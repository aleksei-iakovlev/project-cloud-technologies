import json
from datetime import datetime
from logging import Logger
from lib.redis import RedisClient
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        i = 0

        while i < self._batch_size:

            message_in = self._consumer.consume()
            if message_in is None:
                self._logger.info("No message received from Kafka")
                return

            object_id = message_in["object_id"]
            object_type = message_in["object_type"]
            sent_dttm = datetime.fromisoformat(message_in["sent_dttm"])
            order_payload = message_in["payload"]
            payload_json = json.dumps(order_payload)
            self._stg_repository.order_events_insert(
                object_id, object_type, sent_dttm, payload_json
            )

            user_id = order_payload["user"]["id"]
            restaurant_id = order_payload["restaurant"]["id"]
            restaurant_name = self._redis.get(restaurant_id)["name"]
            menu = self._redis.get(restaurant_id)["menu"]
            categories = {
                    item["_id"]: item["category"]
                    for item in menu
                    }

            products = [
                {
                    "id": item["id"],
                    "price": item["price"],
                    "quantity": item["quantity"],
                    "name": item["name"],
                    "category": categories.get(item["id"])
                }
                for item in order_payload["order_items"]
            ]

            message_out = {
                "object_id": object_id,
                "object_type": object_type,
                "payload": {
                    "id": object_id,
                    "date": order_payload["date"],
                    "cost": order_payload["cost"],
                    "payment": order_payload["payment"],
                    "status": order_payload["final_status"],
                    "restaurant": {
                        "id": restaurant_id,
                        "name": restaurant_name,
                    },
                    "user": {
                        "id": user_id,
                        "name": self._redis.get(user_id)["name"],
                    },
                    "products": products,
                },
            }

            self._producer.produce(message_out)

            i += 1

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
