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

            self._dds_repository.insert_h_restaurant(restaurant_id)
            self._dds_repository.insert_s_restaurant_names(restaurant_id, restaurant_name)

            for item in products:
                category_name = item["category"]
                product_id = item["id"]
                self._dds_repository.insert_h_category(category_name)
                self._dds_repository.insert_h_product(product_id)
                self._dds_repository.insert_l_product_restaurant(restaurant_id, product_id)

            i += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH")
