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

            self._dds_repository.insert_h_restaurant(restaurant_id)
            self._dds_repository.insert_s_restaurant_names(restaurant_id, restaurant_name)

            self._dds_repository.insert_s_user_names(user_id, user_name)

            self._dds_repository.insert_h_order(order_id, order_dt)

            self._dds_repository.insert_s_order_cost(order_id, order_cost, order_payment)

            self._dds_repository.insert_s_order_status(order_id, order_status)

            for item in products:
                category_name = item["category"]
                product_id = item["id"]
                product_name = item["name"]
                self._dds_repository.insert_h_category(category_name)
                self._dds_repository.insert_h_product(product_id)
                self._dds_repository.insert_l_product_restaurant(restaurant_id, product_id)
                self._dds_repository.insert_l_product_category(category_name, product_id)
                self._dds_repository.insert_s_product_names(product_id, product_name)

            i += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH")
