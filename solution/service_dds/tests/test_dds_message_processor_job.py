import copy
import json
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from dds_loader.dds_message_processor_job import DdsMessageProcessor


EXAMPLE_MESSAGE_PATH = Path(__file__).resolve().parents[1] / "example_input_message.json"


@pytest.fixture
def example_message():
    with EXAMPLE_MESSAGE_PATH.open(encoding="utf-8") as message_file:
        return json.load(message_file)


@pytest.fixture
def processor_dependencies():
    return {
        "consumer": MagicMock(),
        "producer": MagicMock(),
        "repository": MagicMock(),
        "logger": MagicMock(),
    }


def make_processor(dependencies):
    return DdsMessageProcessor(
        dependencies["consumer"],
        dependencies["producer"],
        dependencies["repository"],
        dependencies["logger"],
    )


def expected_batch_call(message):
    payload = message["payload"]
    products = payload["products"]
    return call.load_batch(
        [product["category"] for product in products],
        [product["id"] for product in products],
        payload["restaurant"]["id"],
        payload["user"]["id"],
        payload["id"],
        payload["date"],
        payload["restaurant"]["name"],
        [product["name"] for product in products],
        payload["user"]["name"],
        payload["user"]["login"],
        payload["cost"],
        payload["payment"],
        payload["status"],
    )


def test_run_loads_example_message_and_preserves_product_alignment(
    example_message, processor_dependencies
):
    dependencies = processor_dependencies
    dependencies["consumer"].consume.side_effect = [example_message, None]
    processor = make_processor(dependencies)

    processor.run()

    repository = dependencies["repository"]
    assert repository.method_calls == [expected_batch_call(example_message)]
    batch_args = repository.load_batch.call_args.args
    products = example_message["payload"]["products"]

    assert len(products) == 5
    assert batch_args[0] == [product["category"] for product in products]
    assert batch_args[1] == [product["id"] for product in products]
    assert batch_args[7] == [product["name"] for product in products]
    dependencies["consumer"].consume.assert_has_calls([call.consume(), call.consume()])


def test_run_processes_each_message_with_its_own_values(
    example_message, processor_dependencies
):
    first_message = copy.deepcopy(example_message)
    second_message = copy.deepcopy(example_message)
    second_message["payload"]["id"] = 987654
    second_message["payload"]["products"][0]["id"] = "different-product"
    dependencies = processor_dependencies
    dependencies["consumer"].consume.side_effect = [first_message, second_message, None]
    processor = make_processor(dependencies)

    processor.run()

    assert dependencies["repository"].method_calls == [
        expected_batch_call(first_message),
        expected_batch_call(second_message),
    ]
    assert dependencies["consumer"].consume.call_count == 3


def test_run_stops_when_consumer_has_no_message(processor_dependencies):
    dependencies = processor_dependencies
    dependencies["consumer"].consume.return_value = None
    processor = make_processor(dependencies)

    processor.run()

    dependencies["repository"].load_batch.assert_not_called()
    dependencies["consumer"].consume.assert_called_once_with()
    dependencies["logger"].info.assert_any_call("No message received from Kafka")


def test_run_finishes_current_batch_when_consumer_returns_none_after_message(
    example_message, processor_dependencies
):
    dependencies = processor_dependencies
    dependencies["consumer"].consume.side_effect = [example_message, None]
    processor = make_processor(dependencies)

    processor.run()

    assert dependencies["repository"].method_calls == [
        expected_batch_call(example_message)
    ]
    assert dependencies["consumer"].consume.call_count == 2


def test_run_does_not_exceed_batch_size(example_message, processor_dependencies):
    dependencies = processor_dependencies
    dependencies["consumer"].consume.return_value = example_message
    processor = make_processor(dependencies)

    processor.run()

    assert dependencies["consumer"].consume.call_count == 30
    assert dependencies["repository"].load_batch.call_count == 30


def test_run_propagates_key_error_for_missing_required_field(
    example_message, processor_dependencies
):
    del example_message["payload"]["user"]["login"]
    dependencies = processor_dependencies
    dependencies["consumer"].consume.return_value = example_message
    processor = make_processor(dependencies)

    with pytest.raises(KeyError, match="login"):
        processor.run()

    dependencies["repository"].load_batch.assert_not_called()
