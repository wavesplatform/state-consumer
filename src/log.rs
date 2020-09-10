use once_cell::sync::Lazy;
use slog::{o, Drain, FnValue, Logger, PushFnValue, Record};
use std::sync::Mutex;

pub static APP_LOG: Lazy<slog::Logger> = Lazy::new(|| init_logger());

fn init_logger() -> Logger {
    let drain = slog_json::Json::new(std::io::stdout()).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();

    slog::Logger::root(
        Mutex::new(drain).map(slog::Fuse),
        o!(
            "ts" => PushFnValue(move |_: &Record, ser| {
                ser.emit(chrono::Local::now().to_rfc3339())
            }),
            "lvl" => FnValue(move |rec: &Record| {
                rec.level().as_short_str()
            }),
            "loc" => FnValue(move |rec: &Record| {
                format!("{}:{}", rec.module(), rec.line())
            }),
            "msg" => PushFnValue(move |rec: &Record, ser| {
                ser.emit(rec.msg())
            }),
            "v" => env!("CARGO_PKG_VERSION"),
        ),
    )
}
