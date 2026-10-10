from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


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

    def pars_input_message(self, input_message):

        payload = input_message["payload"]
        self.object_id = input_message["object_id"]
        self.object_type = input_message["object_type"]

        self.products = payload["products"]
        self.product_ids, self.product_names, self.category_names = [], [], []
        for item in self.products:
            self.category_names.append(item["category"])
            self.product_ids.append(item["id"])
            self.product_names.append(item["name"])

        self.restaurant_id = payload["restaurant"]["id"]
        self.restaurant_name = payload["restaurant"]["name"]
        self.user_id = payload["user"]["id"]
        self.user_name = payload["user"]["name"]
        self.user_login = payload["user"]["login"]
        self.order_id = payload["id"]
        self.order_dt = payload["date"]
        self.order_cost = payload["cost"]
        self.order_payment = payload["payment"]
        self.order_status = payload["status"]

    def pars_output_message(self):

        products = [
            {
                "product_id": item["id"],
                "product_name": item["name"],
                "category_name": item["category"]
            } for item in self.products
        ]
        return {
            "object_id": self.object_id,
            "object_type": self.object_type,
            "payload": {
                "order_id": self.order_id,
                "user_id": self.user_id,
                "products": products
                }
            }

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        i = 0

        while i < self._batch_size:

            input_message = self._consumer.consume()
            if input_message is None:
                self._logger.info("No message received from Kafka")
                return

            self.pars_input_message(input_message)

            self._dds_repository.load_batch(
                self.category_names,
                self.product_ids,
                self.restaurant_id,
                self.user_id,
                self.order_id,
                self.order_dt,
                self.restaurant_name,
                self.product_names,
                self.user_name,
                self.user_login,
                self.order_cost,
                self.order_payment,
                self.order_status
            )

            output_message = self.pars_output_message()

            self._producer.produce(output_message)

            i += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH")
