DROP TABLE IF EXISTS stg.order_events;
create table if not exists stg.order_events (
	id integer generated always as identity primary key,
	object_id integer not null unique,
	object_type varchar not null,
	sent_dttm timestamp not null,
	payload json not null
);