use crate::operation::Op;
use std::collections::BTreeMap;

pub struct InMemoryLayer {
    store: BTreeMap<String, String>,
}

impl InMemoryLayer {
    pub fn new() -> Self {
        Self {
            store: BTreeMap::new(),
        }
    }

    fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    fn del(&mut self, key: String) -> Option<String> {
        self.store.remove(&key)
    }

    pub fn eval(&mut self, op: Op) -> Option<String> {
        let result = match op {
            Op::SET { key, value, .. } => {
                self.set(key.clone(), value);
                Some(key)
            }
            Op::GET { key, .. } => self.get(key),
            Op::DEL { key, .. } => self.del(key),
        };
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_layer() {
        let mut layer = InMemoryLayer::new();
        let op = Op::new_set(0, "key".to_string(), "value".to_string());
        assert_eq!(layer.eval(op), Some("key".to_string()));

        let op = Op::new_get(0, "key".to_string());
        assert_eq!(layer.eval(op), Some("value".to_string()));

        let op = Op::new_del(0, "key".to_string());
        assert_eq!(layer.eval(op), Some("value".to_string()));

        let op = Op::new_get(0, "key".to_string());
        assert_eq!(layer.eval(op), None);
    }
}
