from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect, namespace_uuid) -> None:
        self._db = db
        self._namespace_uuid = namespace_uuid

    def load_batch(self, user_id, products) -> None:
        product_ids = [product["product_id"] for product in products]
        product_names = [product["product_name"] for product in products]
        category_names = [product["category_name"] for product in products]

        statements = [
            (
                """
            INSERT INTO cdm.user_product_counters (
                user_id,
                product_id,
                product_name,
                order_cnt
            )
            SELECT
                %(user_id)s::uuid,
                uuid_generate_v5(%(namespace_uuid)s::uuid, products.product_id),
                products.product_name,
                1
            FROM unnest(
                %(product_ids)s::text[],
                %(product_names)s::text[],
                %(category_names)s::text[]
            ) AS products(product_id, product_name, category_name)
            ON CONFLICT (user_id, product_id)
            DO UPDATE SET
                order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt;
            """,
                {
                    "user_id": user_id,
                    "namespace_uuid": self._namespace_uuid,
                    "product_ids": product_ids,
                    "product_names": product_names,
                    "category_names": category_names,
                },
            ),
            (
                """
            INSERT INTO cdm.user_category_counters (
                user_id,
                category_id,
                category_name,
                order_cnt)
            SELECT
                %(user_id)s::uuid,
                uuid_generate_v5(%(namespace_uuid)s::uuid, products.category_name),
                products.category_name,
                1
            FROM unnest(
                %(product_ids)s::text[],
                %(product_names)s::text[],
                %(category_names)s::text[]
            ) AS products(product_id, product_name, category_name)
            ON CONFLICT (user_id, category_id)
            DO UPDATE SET
                order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt;
            """,
                {
                    "namespace_uuid": self._namespace_uuid,
                    "user_id": user_id,
                    "product_ids": product_ids,
                    "product_names": product_names,
                    "category_names": category_names,
                },
            ),
        ]

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for statement, params in statements:
                    cur.execute(statement, params)
