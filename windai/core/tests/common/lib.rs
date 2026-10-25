//! 本模块提供各集成测试共用的夹具。

use tempfile::TempDir;
use wind_core::db::Db;
use wind_core::storage::Storage;

/// 建立表已就绪、文件位于临时目录的空库
///
/// 返回的临时目录须由调用方持有。临时目录被丢弃时，库文件随之删除
pub async fn store() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    (dir, Storage::new(db))
}
