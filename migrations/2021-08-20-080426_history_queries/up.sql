CREATE TABLE IF NOT EXISTS data_entries_history_keys(
    uid BIGINT GENERATED BY DEFAULT AS IDENTITY CONSTRAINT data_entries_history_keys_uid_key PRIMARY KEY,
    block_uid BIGINT NOT NULL,
    height INTEGER,
    data_entry_uid BIGINT NOT NULL,
    address TEXT NOT NULL,
    key TEXT NOT NULL,
    block_timestamp timestamp
);

-- fill data_entries_history_keys only if no data in there
DO $$
declare V_UID BIGINT;

BEGIN
    SELECT uid into V_UID FROM data_entries_history_keys order by uid desc limit 1;

    IF V_UID IS NULL THEN
        insert into data_entries_history_keys(block_uid, height, data_entry_uid, address, key, block_timestamp)
        select b.uid as block_uid, b.height, d.uid as data_entry_uid, d.address, d.key, to_timestamp(b.time_stamp / 1000) as block_timestamp
        from data_entries d
                inner join blocks_microblocks b on d.block_uid = b.uid
        where (d.value_binary IS NOT NULL OR d.value_bool IS NOT NULL OR d.value_integer IS NOT NULL OR d.value_string IS NOT NULL)
        ;

    END IF;
END;
$$;


CREATE INDEX IF NOT EXISTS data_entries_history_keys_block_uid_idx on data_entries_history_keys (block_uid);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'data_entries_history_keys_block_uid_fkey') THEN
        alter table data_entries_history_keys add foreign key (block_uid) references blocks_microblocks(uid) on delete cascade;    
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS  "data_entries_history_keys_stamp_idx" on data_entries_history_keys(address, key, block_timestamp desc, data_entry_uid desc);
CREATE INDEX IF NOT EXISTS  "data_entries_history_keys_height_idx" on data_entries_history_keys(address, key, height desc, data_entry_uid desc);
