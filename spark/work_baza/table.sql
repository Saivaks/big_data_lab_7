CREATE TABLE IF NOT EXISTS test.data (
id text,
popularity_key text,
complete int,
nutrition_score_beverage int,
pnns_groups_2 text,
lc text,
created_t text,
countries text,
last_modified_t text,
pnns_groups_1 text,
creator text,
nutrition_data_per text,
lang text,
rev int,
nutrition_data_prepared_per text,
update_key text,
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS test.result (
id text,
clust int,
PRIMARY KEY(id)
);