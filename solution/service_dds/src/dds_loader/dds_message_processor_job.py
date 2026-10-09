from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

import json


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository

        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        i = 0

        while i < self._batch_size:

            input_message = self._consumer.consume()
            if input_message is None:
                self._logger.info("No message received from Kafka")
                return

            payload: dict = input_message["payload"]
            products: list[dict] = payload["products"]
            restaurant_id = payload["restaurant"]["id"]
            restaurant_name = payload["restaurant"]["name"]
            user_id = payload["user"]["id"]
            user_name = payload["user"]["name"]
            order_id = payload["id"]
            order_dt = payload["date"]
            order_cost = payload["cost"]
            order_payment = payload["payment"]
            order_status = payload["status"]

            category_names, product_ids, product_names = [], [], []
            for item in products:
                category_names.append(item["category"])
                product_ids.append(item["id"])
                product_names.append(item["name"])

            i += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH")
