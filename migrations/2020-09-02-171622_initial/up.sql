CREATE TABLE IF NOT EXISTS last_handled_height (
  height INT PRIMARY KEY
);

INSERT INTO
    last_handled_height
VALUES
    (0);
    
CREATE TABLE IF NOT EXISTS data_entries (
    address VARCHAR NOT NULL,
    key VARCHAR NOT NULL,
    height INT NOT NULL,
    value_binary TEXT,
    value_bool BOOLEAN,
    value_integer BIGINT,
    value_string VARCHAR,
    fragment_0_string VARCHAR,
    fragment_0_integer INT,
    fragment_1_string VARCHAR,
    fragment_1_integer INT,
    fragment_2_string VARCHAR,
    fragment_2_integer INT,
    fragment_3_string VARCHAR,
    fragment_3_integer INT,
    fragment_4_string VARCHAR,
    fragment_4_integer INT,
    fragment_5_string VARCHAR,
    fragment_5_integer INT,
    fragment_6_string VARCHAR,
    fragment_6_integer INT,
    fragment_7_string VARCHAR,
    fragment_7_integer INT,
    fragment_8_string VARCHAR,
    fragment_8_integer INT,
    fragment_9_string VARCHAR,
    fragment_9_integer INT,
    fragment_10_string VARCHAR,
    fragment_10_integer INT
);

ALTER TABLE data_entries
    ADD CONSTRAINT data_entries_pkey
    PRIMARY KEY (address, key);

CREATE INDEX data_entries_fragment_0_string_idx ON data_entries (fragment_0_string);
CREATE INDEX data_entries_fragment_0_integer_idx ON data_entries (fragment_0_integer);
CREATE INDEX data_entries_fragment_1_string_idx ON data_entries (fragment_1_string);
CREATE INDEX data_entries_fragment_1_integer_idx ON data_entries (fragment_1_integer);
CREATE INDEX data_entries_fragment_2_string_idx ON data_entries (fragment_2_string);
CREATE INDEX data_entries_fragment_2_integer_idx ON data_entries (fragment_2_integer);
CREATE INDEX data_entries_fragment_3_string_idx ON data_entries (fragment_3_string);
CREATE INDEX data_entries_fragment_3_integer_idx ON data_entries (fragment_3_integer);
CREATE INDEX data_entries_fragment_4_string_idx ON data_entries (fragment_4_string);
CREATE INDEX data_entries_fragment_4_integer_idx ON data_entries (fragment_4_integer);
CREATE INDEX data_entries_fragment_5_string_idx ON data_entries (fragment_5_string);
CREATE INDEX data_entries_fragment_5_integer_idx ON data_entries (fragment_5_integer);
CREATE INDEX data_entries_fragment_6_string_idx ON data_entries (fragment_6_string);
CREATE INDEX data_entries_fragment_6_integer_idx ON data_entries (fragment_6_integer);
CREATE INDEX data_entries_fragment_7_string_idx ON data_entries (fragment_7_string);
CREATE INDEX data_entries_fragment_7_integer_idx ON data_entries (fragment_7_integer);
CREATE INDEX data_entries_fragment_8_string_idx ON data_entries (fragment_8_string);
CREATE INDEX data_entries_fragment_8_integer_idx ON data_entries (fragment_8_integer);
CREATE INDEX data_entries_fragment_9_string_idx ON data_entries (fragment_9_string);
CREATE INDEX data_entries_fragment_9_integer_idx ON data_entries (fragment_9_integer);
CREATE INDEX data_entries_fragment_10_string_idx ON data_entries (fragment_10_string);
CREATE INDEX data_entries_fragment_10_integer_idx ON data_entries (fragment_10_integer);
CREATE INDEX data_entries_key_idx ON data_entries(key);
CREATE INDEX data_entries_value_integer_idx ON data_entries(value_integer);
CREATE INDEX data_entries_value_string_idx ON data_entries(value_string);
