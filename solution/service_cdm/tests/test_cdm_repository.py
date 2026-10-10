from unittest.mock import MagicMock

import pytest

from cdm_loader.repository.cdm_repository import CdmRepository


@pytest.fixture
def repository_and_cursor():
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    db = MagicMock()
    db.connection.return_value.__enter__.return_value = connection
    return CdmRepository(db), cursor


def test_load_batch_unnests_aligned_product_columns(repository_and_cursor):
    repository, cursor = repository_and_cursor
    products = [
        {
            "product_id": "product-1",
            "product_name": "First item",
            "category_name": "Category A",
        },
        {
            "product_id": "product-2",
            "product_name": "Second item",
            "category_name": "Category B",
        },
    ]

    repository.load_batch("11111111-1111-1111-1111-111111111111", products)

    assert cursor.execute.call_count == 2
    first_query, first_params = cursor.execute.call_args_list[0].args
    second_query, second_params = cursor.execute.call_args_list[1].args

    assert "unnest(" in first_query
    assert "%(product_ids)s::text[]" in first_query
    assert "%(product_names)s::text[]" in first_query
    assert "%(category_names)s::text[]" in first_query
    assert "AS products(product_id, product_name, category_name)" in first_query
    assert "%(products)s" not in first_query
    assert "unnest(" in second_query
    assert "AS products(product_id, product_name, category_name)" in second_query

    expected_arrays = {
        "product_ids": ["product-1", "product-2"],
        "product_names": ["First item", "Second item"],
        "category_names": ["Category A", "Category B"],
    }
    for params in (first_params, second_params):
        assert {key: params[key] for key in expected_arrays} == expected_arrays
        assert params["user_id"] == "11111111-1111-1111-1111-111111111111"
    assert first_params["namespace_uuid"] == repository._namespace_uuid
    assert second_params["namespace_uuid"] == repository._namespace_uuid


def test_load_batch_accepts_no_products(repository_and_cursor):
    repository, cursor = repository_and_cursor

    repository.load_batch("11111111-1111-1111-1111-111111111111", [])

    assert cursor.execute.call_count == 2
    for executed_call in cursor.execute.call_args_list:
        _, params = executed_call.args
        assert params["product_ids"] == []
        assert params["product_names"] == []
        assert params["category_names"] == []
