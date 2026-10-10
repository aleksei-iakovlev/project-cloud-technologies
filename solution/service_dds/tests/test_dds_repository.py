from unittest.mock import MagicMock

import pytest
from psycopg import sql as psycopg_sql

from dds_loader.repository.dds_repository import DdsRepository


@pytest.fixture
def repository_and_cursor():
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    db = MagicMock()
    db.connection.return_value.__enter__.return_value = connection
    return DdsRepository(db, "11111111-1111-1111-1111-111111111111"), db, connection, cursor


def assert_batch_executions(repository, cursor, expected):
    assert cursor.execute.call_count == len(expected)
    for executed_call, (target, keys) in zip(cursor.execute.call_args_list, expected):
        query, params = executed_call.args
        assert isinstance(query, psycopg_sql.SQL)
        statement = query.as_string(None)
        assert f"INSERT INTO dds.{target}" in statement
        assert statement.count("INSERT INTO") == 1
        assert set(params) == set(keys)
        assert params["namespace_uuid"] == repository._namespace_uuid
        assert params["load_src"] == "stg_kafka"


def test_load_batch_executes_all_inserts_in_one_connection(repository_and_cursor):
    repository, db, connection, cursor = repository_and_cursor
    category_names = ["category-a", "category-b"]
    product_ids = ["product-a", "product-b"]
    product_names = ["name-a", "name-b"]

    repository.load_batch(
        category_names,
        product_ids,
        "restaurant-a",
        "user-a",
        123,
        "2026-01-02",
        "Restaurant",
        product_names,
        "User",
        "user-login",
        45.5,
        40.0,
        "CLOSED",
    )

    assert db.connection.call_count == 1
    connection.cursor.assert_called_once_with()
    assert_batch_executions(repository, cursor, [
        ("h_category", {"namespace_uuid", "category_names", "load_src"}),
        ("h_product", {"namespace_uuid", "product_ids", "load_src"}),
        ("h_restaurant", {"namespace_uuid", "restaurant_id", "load_src"}),
        ("h_user", {"namespace_uuid", "user_id", "load_src"}),
        ("h_order", {"namespace_uuid", "order_id", "order_dt", "load_src"}),
        ("l_order_product", {"namespace_uuid", "load_src", "product_ids", "order_id"}),
        ("l_order_user", {"namespace_uuid", "load_src", "order_id", "user_id"}),
        ("l_product_restaurant", {"namespace_uuid", "load_src", "product_ids", "restaurant_id"}),
        ("l_product_category", {"namespace_uuid", "load_src", "product_ids", "category_names"}),
        ("s_restaurant_names", {"namespace_uuid", "restaurant_id", "restaurant_name", "load_src"}),
        ("s_product_names", {"namespace_uuid", "product_ids", "product_names", "load_src"}),
        ("s_user_names", {"namespace_uuid", "user_id", "user_name", "user_login", "load_src"}),
        ("s_order_cost", {"namespace_uuid", "order_id", "order_cost", "order_payment", "load_src"}),
        ("s_order_status", {"namespace_uuid", "order_id", "order_status", "load_src"}),
    ])
    assert cursor.execute.call_args_list[0].args[1]["category_names"] == category_names
    assert cursor.execute.call_args_list[1].args[1]["product_ids"] == product_ids
    assert cursor.execute.call_args_list[4].args[1]["order_dt"] == "2026-01-02"
    assert cursor.execute.call_args_list[9].args[1]["restaurant_name"] == "Restaurant"
    assert cursor.execute.call_args_list[10].args[1]["product_names"] == product_names
    assert cursor.execute.call_args_list[11].args[1]["user_login"] == "user-login"
    assert cursor.execute.call_args_list[12].args[1]["order_cost"] == 45.5
    assert cursor.execute.call_args_list[12].args[1]["order_payment"] == 40.0
    assert cursor.execute.call_args_list[13].args[1]["order_status"] == "CLOSED"


def test_load_batch_propagates_execute_error(repository_and_cursor):
    repository, _, _, cursor = repository_and_cursor
    cursor.execute.side_effect = RuntimeError("database error")

    with pytest.raises(RuntimeError, match="database error"):
        repository.load_batch([], [], "restaurant", "user", 1, "date", "Restaurant", [], "User", "login", 1, 1, "status")

    cursor.execute.assert_called_once()
