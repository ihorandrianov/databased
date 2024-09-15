use hashlink::{linked_hash_map::RawEntryMut, LinkedHashMap};

pub struct LruCacheLayer {
    cache: LinkedHashMap<String, String>,
}

impl LruCacheLayer {
    pub fn new(capacity: u32) -> Self {
        let cache = LinkedHashMap::with_capacity(capacity as usize + 1);

        Self { cache }
    }

    fn put(&mut self, key: &str, value: &str) -> () {
        let _cached_val = match self.cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut occupied) => {
                occupied.to_back();
                occupied.into_mut()
            }
            RawEntryMut::Vacant(vacant) => vacant.insert(key.to_owned(), value.to_owned()).1,
        };
        if self.cache.capacity() == self.cache.len() {
            self.cache.pop_front();
        }
    }

    fn get(&mut self, key: &str) -> Option<String> {
        if let RawEntryMut::Occupied(mut entry) = self.cache.raw_entry_mut().from_key(key) {
            entry.to_back();
            return Some(entry.get().clone());
        }
        None
    }

    fn del(&mut self, key: &str) -> () {
        if let RawEntryMut::Occupied(entry) = self.cache.raw_entry_mut().from_key(key) {
            entry.remove_entry();
        };
    }
}
