from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def pars_input_message(self, input_message):

        payload = input_message["payload"]
        self.order_id = payload["order_id"]
        self.user_id = payload["user_id"]
        self.products = payload["products"]

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        i = 0

        while i < self._batch_size:

            input_message = self._consumer.consume()
            if input_message is None:
                self._logger.info("No message received from Kafka")
                return

            self.pars_input_message(input_message)

            self._cdm_repository.load_batch(self.user_id, self.products)

            i += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH")
