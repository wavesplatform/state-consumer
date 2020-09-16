table! {
    blocks_microblocks (id) {
        uid -> BigInt,
        id -> Varchar,
        height -> Int4,
        time_stamp -> Nullable<BigInt>,
    }
}

table! {
    data_entries_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    data_entries (superseded_by, address, key) {
        block_uid -> BigInt,
        transaction_id -> Varchar,
        uid -> BigInt,
        superseded_by -> BigInt,
        address -> Varchar,
        key -> Varchar,
        value_binary -> Nullable<Binary>,
        value_bool -> Nullable<Bool>,
        value_integer -> Nullable<BigInt>,
        value_string -> Nullable<Varchar>,
        fragment_0_integer -> Nullable<Int4>,
        fragment_0_string -> Nullable<Varchar>,
        fragment_1_integer -> Nullable<Int4>,
        fragment_1_string -> Nullable<Varchar>,
        fragment_2_integer -> Nullable<Int4>,
        fragment_2_string -> Nullable<Varchar>,
        fragment_3_integer -> Nullable<Int4>,
        fragment_3_string -> Nullable<Varchar>,
        fragment_4_integer -> Nullable<Int4>,
        fragment_4_string -> Nullable<Varchar>,
        fragment_5_integer -> Nullable<Int4>,
        fragment_5_string -> Nullable<Varchar>,
        fragment_6_integer -> Nullable<Int4>,
        fragment_6_string -> Nullable<Varchar>,
        fragment_7_integer -> Nullable<Int4>,
        fragment_7_string -> Nullable<Varchar>,
        fragment_8_integer -> Nullable<Int4>,
        fragment_8_string -> Nullable<Varchar>,
        fragment_9_integer -> Nullable<Int4>,
        fragment_9_string -> Nullable<Varchar>,
        fragment_10_integer -> Nullable<Int4>,
        fragment_10_string -> Nullable<Varchar>,
    }
}
