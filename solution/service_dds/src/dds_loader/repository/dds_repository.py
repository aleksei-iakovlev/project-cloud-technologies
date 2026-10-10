from psycopg import sql

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect, namespace_uuid) -> None:
        self._db = db
        self._load_src = "stg_kafka"
        self._namespace_uuid = namespace_uuid

    def load_batch(
        self,
        category_names,
        product_ids,
        restaurant_id,
        user_id,
        order_id,
        order_dt,
        restaurant_name,
        product_names,
        user_name,
        user_login,
        order_cost,
        order_payment,
        order_status
    ) -> None:
        statements = [
            (
                """
            INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
            SELECT DISTINCT
                uuid_generate_v5(%(namespace_uuid)s::uuid, categories.category_name),
                categories.category_name,
                now(),
                %(load_src)s
            FROM unnest(%(category_names)s::text[]) AS categories(category_name)
            ON CONFLICT (h_category_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "category_names": category_names,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
            SELECT DISTINCT
                uuid_generate_v5(%(namespace_uuid)s::uuid, products.product_id),
                products.product_id,
                now(),
                %(load_src)s
            FROM unnest(%(product_ids)s::text[]) AS products(product_id)
            ON CONFLICT (h_product_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "product_ids": product_ids,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
            VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(restaurant_id)s::text),
                %(restaurant_id)s,
                now(),
                %(load_src)s)
            ON CONFLICT (h_restaurant_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "restaurant_id": restaurant_id,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
            VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(user_id)s::text),
                %(user_id)s, now(), %(load_src)s)
            ON CONFLICT (h_user_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "user_id": user_id,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
            VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(order_id)s::text),
                %(order_id)s, %(order_dt)s, now(), %(load_src)s)
            ON CONFLICT (h_order_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "order_id": order_id,
                    "order_dt": order_dt,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.l_order_product (
                hk_order_product_pk,
                h_order_pk,
                h_product_pk,
                load_dt,
                load_src
                )
            SELECT DISTINCT
                uuid_generate_v5(%(namespace_uuid)s::uuid, ho.h_order_pk::text || hp.h_product_pk::text),
                ho.h_order_pk,
                hp.h_product_pk,
                now(),
                %(load_src)s
            FROM unnest(%(product_ids)s::text[]) AS u(product_id)
                JOIN dds.h_product hp
                    ON uuid_generate_v5(%(namespace_uuid)s::uuid, u.product_id) = hp.h_product_pk
                JOIN dds.h_order ho
                    ON uuid_generate_v5(%(namespace_uuid)s::uuid, %(order_id)s::text) = ho.h_order_pk
            ON CONFLICT (hk_order_product_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "load_src": self._load_src,
                    "product_ids": product_ids,
                    "order_id": order_id,
                },
            ),
            (
                """
            INSERT INTO dds.l_order_user (
                hk_order_user_pk,
                h_user_pk,
                h_order_pk,
                load_dt,
                load_src
                )
            SELECT DISTINCT
                uuid_generate_v5(%(namespace_uuid)s::uuid, hu.h_user_pk::text || ho.h_order_pk::text),
                hu.h_user_pk,
                ho.h_order_pk,
                now(),
                %(load_src)s
            FROM dds.h_order ho JOIN dds.h_user hu
                ON uuid_generate_v5(%(namespace_uuid)s::uuid, %(order_id)s)::text) = ho.h_order_pk
                AND uuid_generate_v5(%(namespace_uuid)s::uuid, %(user_id)s::text) = ho.h_user_pk
            ON CONFLICT (hk_order_user_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "load_src": self._load_src,
                    "order_id": order_id,
                    "user_id": user_id,
                },
            ),
            (
                """
            INSERT INTO dds.l_product_restaurant (
                hk_product_restaurant_pk,
                h_product_pk,
                h_restaurant_pk,
                load_dt,
                load_src
                )
            SELECT DISTINCT
                uuid_generate_v5(%(namespace_uuid)s::uuid, hp.h_product_pk::text || hr.h_restaurant_pk::text),
                hp.h_product_pk,
                hr.h_restaurant_pk,
                now(),
                %(load_src)s
            FROM unnest(%(product_ids)s::text[]) AS u(product_id)
                JOIN dds.h_product hp
                    ON uuid_generate_v5(%(namespace_uuid)s::uuid, u.product_id) = hp.h_product_pk
                JOIN dds.h_restaurant hr
                    ON uuid_generate_v5(%(namespace_uuid)s::uuid, %(restaurant_id)s::text) = hr.h_restaurant_pk
            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "load_src": self._load_src,
                    "product_ids": product_ids,
                    "restaurant_id": restaurant_id,
                },
            ),
            (
                """
            INSERT INTO dds.l_product_category (
                hk_product_category_pk,
                h_product_pk,
                h_category_pk,
                load_dt,
                load_src
                )
            SELECT DISTINCT
                uuid_generate_v5(%(namespace_uuid)s::uuid, hp.h_product_pk::text || hc.h_category_pk::text),
                hp.h_product_pk,
                hc.h_category_pk,
                now(),
                %(load_src)s
            FROM unnest(%(product_ids)s::text[], %(category_names)s::text[]) AS u(product_id, category_name)
                JOIN dds.h_product hp
                    ON uuid_generate_v5(%(namespace_uuid)s::uuid, u.product_id) = hp.h_product_pk
                JOIN dds.h_category hc
                    ON uuid_generate_v5(%(namespace_uuid)s::uuid, u.category_name) = hc.h_category_pk
            ON CONFLICT (hk_product_category_pk) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "load_src": self._load_src,
                    "product_ids": product_ids,
                    "category_names": category_names,
                },
            ),
            (
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
            ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "restaurant_id": restaurant_id,
                    "restaurant_name": restaurant_name,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.s_product_names (
                h_product_pk,
                name,
                load_dt,
                load_src,
                hk_product_names_hashdiff
                )
            SELECT DISTINCT
                uuid_generate_v5(%(namespace_uuid)s::uuid, u.product_id),
                u.product_name,
                now(),
                %(load_src)s,
                uuid_generate_v5(%(namespace_uuid)s::uuid, u.product_id || u.product_name)
            FROM unnest(%(product_ids)s::text[], %(product_names)s::text[]) AS u(product_id, product_name)
            ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "product_ids": product_ids,
                    "product_names": product_names,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src,
                hk_user_names_hashdiff)
            VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(user_id)s::text),
                %(user_name)s, %(user_login)s, now(), %(load_src)s,
                uuid_generate_v5(%(namespace_uuid)s::uuid, %(user_id)s::text || %(user_name)s::text))
            ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_login": user_login,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src,
                hk_order_cost_hashdiff)
            VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(order_id)s::text),
                %(order_cost)s, %(order_payment)s, now(), %(load_src)s,
                uuid_generate_v5(%(namespace_uuid)s::uuid, %(order_id)s::text || %(order_cost)s::text
                    || %(order_payment)s::text))
            ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "order_id": order_id,
                    "order_cost": order_cost,
                    "order_payment": order_payment,
                    "load_src": self._load_src,
                },
            ),
            (
                """
            INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src,
                hk_order_status_hashdiff)
            VALUES (uuid_generate_v5(%(namespace_uuid)s::uuid, %(order_id)s::text),
                %(order_status)s, now(), %(load_src)s,
                uuid_generate_v5(%(namespace_uuid)s::uuid, %(order_id)s::text || %(order_status)s::text))
            ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "order_id": order_id,
                    "order_status": order_status,
                    "load_src": self._load_src,
                },
            )
        ]

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for statement, params in statements:
                    cur.execute(sql.SQL(statement), params)
