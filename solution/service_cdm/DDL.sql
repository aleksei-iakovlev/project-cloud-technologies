DROP TABLE IF EXISTS cdm.user_product_counters;
CREATE TABLE IF NOT EXISTS cdm.user_product_counters (
	id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	user_id uuid NOT NULL,
	product_id uuid NOT NULL,
	product_name varchar NOT NULL,
	order_cnt integer NOT NULL
);
CREATE INDEX idx_user_id_product_id ON cdm.user_product_counters (user_id, product_id);

DROP TABLE IF EXISTS cdm.user_category_counters;
CREATE TABLE IF NOT EXISTS cdm.user_category_counters (
	id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	user_id uuid NOT NULL,
	category_id uuid NOT NULL,
	category_name varchar NOT NULL,
	order_cnt integer NOT NULL
);
CREATE INDEX idx_category_id_category_id ON cdm.user_category_counters (category_id, category_name);