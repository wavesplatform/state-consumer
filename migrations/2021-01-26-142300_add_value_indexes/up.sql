-- Your SQL goes here
CREATE INDEX IF NOT EXISTS data_entries_expr_ids ON data_entries((1)) WHERE value_bool IS NOT NULL;
CREATE INDEX IF NOT EXISTS data_entries_md5_value_binary_ids ON data_entries(md5(value_binary));