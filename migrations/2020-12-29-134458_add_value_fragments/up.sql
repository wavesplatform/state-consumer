-- Your SQL goes here
ALTER TABLE data_entries
    ADD COLUMN IF NOT EXISTS value_fragment_0_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_0_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_1_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_1_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_2_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_2_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_3_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_3_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_4_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_4_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_5_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_5_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_6_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_6_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_7_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_7_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_8_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_8_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_9_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_9_integer BIGINT,
    ADD COLUMN IF NOT EXISTS value_fragment_10_string VARCHAR,
    ADD COLUMN IF NOT EXISTS value_fragment_10_integer BIGINT;

CREATE INDEX IF NOT EXISTS data_entries_value_fragment_0_string_idx ON data_entries (value_fragment_0_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_0_integer_idx ON data_entries (value_fragment_0_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_1_string_idx ON data_entries (value_fragment_1_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_1_integer_idx ON data_entries (value_fragment_1_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_2_string_idx ON data_entries (value_fragment_2_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_2_integer_idx ON data_entries (value_fragment_2_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_3_string_idx ON data_entries (value_fragment_3_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_3_integer_idx ON data_entries (value_fragment_3_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_4_string_idx ON data_entries (value_fragment_4_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_4_integer_idx ON data_entries (value_fragment_4_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_5_string_idx ON data_entries (value_fragment_5_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_5_integer_idx ON data_entries (value_fragment_5_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_6_string_idx ON data_entries (value_fragment_6_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_6_integer_idx ON data_entries (value_fragment_6_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_7_string_idx ON data_entries (value_fragment_7_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_7_integer_idx ON data_entries (value_fragment_7_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_8_string_idx ON data_entries (value_fragment_8_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_8_integer_idx ON data_entries (value_fragment_8_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_9_string_idx ON data_entries (value_fragment_9_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_9_integer_idx ON data_entries (value_fragment_9_integer);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_10_string_idx ON data_entries (value_fragment_10_string);
CREATE INDEX IF NOT EXISTS data_entries_value_fragment_10_integer_idx ON data_entries (value_fragment_10_integer);