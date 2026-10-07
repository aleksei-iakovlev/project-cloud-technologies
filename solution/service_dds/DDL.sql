TRUNCATE TABLE
    dds.l_order_product,
    dds.l_product_restaurant,
    dds.l_order_user,
    dds.l_product_category,
    dds.s_user_names,
    dds.s_order_cost,
    dds.s_product_names,
    dds.s_order_status,
    dds.s_restaurant_names,
    dds.h_user,
    dds.h_product,
    dds.h_category,
    dds.h_restaurant,
    dds.h_order
CASCADE;

DROP TABLE IF EXISTS dds.l_order_product;
DROP TABLE IF EXISTS dds.l_product_restaurant;
DROP TABLE IF EXISTS dds.l_order_user;
DROP TABLE IF EXISTS dds.l_product_category;
DROP TABLE IF EXISTS dds.s_user_names;
DROP TABLE IF EXISTS dds.s_order_cost;
DROP TABLE IF EXISTS dds.s_product_names;
DROP TABLE IF EXISTS dds.s_order_status;
DROP TABLE IF EXISTS dds.s_restaurant_names;
DROP TABLE IF EXISTS dds.h_user;
DROP TABLE IF EXISTS dds.h_product;
DROP TABLE IF EXISTS dds.h_category;
DROP TABLE IF EXISTS dds.h_restaurant;
DROP TABLE IF EXISTS dds.h_order;

CREATE TABLE dds.h_user (
	h_user_pk uuid PRIMARY KEY,
	user_id varchar NOT NULL UNIQUE,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL
);

CREATE TABLE dds.h_product (
    h_product_pk uuid PRIMARY KEY,
    product_id varchar NOT NULL UNIQUE,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.h_category (
    h_category_pk uuid PRIMARY KEY,
    category_name varchar NOT NULL UNIQUE,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.h_restaurant (
    h_restaurant_pk uuid PRIMARY KEY,
    restaurant_id varchar NOT NULL UNIQUE,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.h_order (
    h_order_pk uuid PRIMARY KEY,
    order_id integer NOT NULL UNIQUE,
    order_dt timestamp NOT NULL,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.l_order_product (
	hk_order_product_pk uuid PRIMARY KEY,
	h_order_pk uuid REFERENCES dds.h_order (h_order_pk) NOT NULL,
	h_product_pk uuid REFERENCES dds.h_product (h_product_pk) NOT NULL,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.l_product_restaurant (
    hk_product_restaurant_pk uuid PRIMARY KEY,
    h_product_pk uuid NOT NULL REFERENCES dds.h_product (h_product_pk),
    h_restaurant_pk uuid NOT NULL REFERENCES dds.h_restaurant (h_restaurant_pk),
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.l_order_user (
    hk_order_user_pk uuid PRIMARY KEY,
    h_user_pk uuid NOT NULL REFERENCES dds.h_user (h_user_pk),
    h_order_pk uuid NOT NULL REFERENCES dds.h_order (h_order_pk),
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.l_product_category (
    hk_product_category_pk uuid PRIMARY KEY,
    h_product_pk uuid NOT NULL REFERENCES dds.h_product (h_product_pk),
    h_category_pk uuid NOT NULL REFERENCES dds.h_category (h_category_pk),
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL
);

CREATE TABLE dds.s_user_names (
	h_user_pk uuid NOT NULL REFERENCES dds.h_user (h_user_pk),
	username varchar NOT NULL,
	userlogin varchar NOT NULL,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL,
    hk_user_names_hashdiff uuid PRIMARY KEY
);

CREATE TABLE dds.s_order_cost (
    h_order_pk uuid NOT NULL REFERENCES dds.h_order (h_order_pk),
    cost decimal(19, 5) NOT NULL CHECK (cost >= 0),
    payment decimal(19, 5) NOT NULL CHECK (payment >= 0),
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL,
    hk_order_cost_hashdiff uuid PRIMARY KEY
);

CREATE TABLE dds.s_product_names (
    h_product_pk uuid NOT NULL REFERENCES dds.h_product (h_product_pk),
    name varchar NOT NULL,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL,
    hk_product_names_hashdiff uuid PRIMARY KEY
);

CREATE TABLE dds.s_order_status (
    h_order_pk uuid NOT NULL REFERENCES dds.h_order (h_order_pk),
    status varchar NOT NULL,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL,
    hk_order_status_hashdiff uuid PRIMARY KEY
);

CREATE TABLE dds.s_restaurant_names (
    h_restaurant_pk uuid NOT NULL REFERENCES dds.h_restaurant (h_restaurant_pk),
    name varchar NOT NULL,
    load_dt timestamp NOT NULL,
    load_src varchar NOT NULL,
    hk_restaurant_names_hashdiff uuid PRIMARY KEY
);
