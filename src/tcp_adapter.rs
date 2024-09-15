use crate::kvstore::KvStore;

pub struct TcpAdapter {
    kv_store: KvStore,
}

impl TcpAdapter {
    pub async fn new(kv_store: KvStore) -> Self {
        Self { kv_store }
    }
}
