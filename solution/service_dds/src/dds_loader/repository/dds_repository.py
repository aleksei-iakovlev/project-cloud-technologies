import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
# from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self._load_src = "stg_kafka"
        self._namespace_uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

    def insert_h_category(self, category_name: str) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                        VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(category_name)s::text), %(category_name)s, now(), %(load_src)s)
                        ON CONFLICT (h_category_pk)
                        DO NOTHING
                    """,
                    {
                        'category_name': category_name,
                        'load_src': self._load_src,
                        'namespace_uuid': self._namespace_uuid
                    }
                )

    def insert_h_product(self, product_id: str) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                        VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(product_id)s::text),
                            %(product_id)s, now(), %(load_src)s)
                        ON CONFLICT (h_product_pk)
                        DO NOTHING
                    """,
                    {
                        'product_id': product_id,
                        'load_src': self._load_src,
                        'namespace_uuid': self._namespace_uuid
                    }
                )

    def insert_h_restaurant(self, restaurant_id: str) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                        VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(restaurant_id)s::text),
                            %(restaurant_id)s, now(), %(load_src)s)
                        ON CONFLICT (h_restaurant_pk)
                        DO NOTHING
                    """,
                    {
                        'restaurant_id': restaurant_id,
                        'load_src': self._load_src,
                        'namespace_uuid': self._namespace_uuid
                    }
                )

    def insert_s_restaurant_names(self, restaurant_id: str, restaurant_name: str) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names (
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                            )
                        VALUES (
                            uuid_generate_v5(%(namespace_uuid)s::uuid, %(restaurant_id)s::text),
                            %(restaurant_name)s::text,
                            now(),
                            %(load_src)s,
                            uuid_generate_v5(%(namespace_uuid)s::uuid, %(restaurant_id)s::text || %(restaurant_name)s::text)
                            )
                        ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING
                    """,
                    {
                        'restaurant_name': restaurant_name,
                        'load_src': self._load_src,
                        'restaurant_id': restaurant_id,
                        'namespace_uuid': self._namespace_uuid
                    }
                )

    def insert_l_product_restaurant(self, restaurant_id, product_id) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant (
                            hk_product_restaurant_pk,
                            h_product_pk,
                            h_restaurant_pk,
                            load_dt,
                            load_src
                            )
                        SELECT
                            uuid_generate_v5(%(namespace_uuid)s::uuid, h_product_pk::text || h_restaurant_pk::text),
                            uuid_generate_v5(%(namespace_uuid)s::uuid, %(product_id)s::text),
                            uuid_generate_v5(%(namespace_uuid)s::uuid, %(restaurant_id)s::text),
                            now(),
                            %(load_src)s
                        FROM dds.h_product hp JOIN dds.h_restaurant hr ON uuid_generate_v5(%(namespace_uuid)s::uuid, %(product_id)s::text) = hp.h_product_pk
                            AND uuid_generate_v5(%(namespace_uuid)s::uuid, %(restaurant_id)s::text) = hr.h_restaurant_pk
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING
                    """,
                    {
                        'namespace_uuid': self._namespace_uuid,
                        'restaurant_id': restaurant_id,
                        'product_id': product_id,
                        'load_src': self._load_src
                    }
                )