use crate::bytecode_serializer::BytecodeSerializer;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Op {
    SET {
        timestamp: i64,
        key: String,
        value: String,
    },
    GET {
        timestamp: i64,
        key: String,
    },
    DEL {
        timestamp: i64,
        key: String,
    },
}

impl Op {
    pub fn new_set(timestamp: i64, key: String, value: String) -> Self {
        Op::SET {
            timestamp,
            key,
            value,
        }
    }
    pub fn new_get(timestamp: i64, key: String) -> Self {
        Op::GET { timestamp, key }
    }
    pub fn new_del(timestamp: i64, key: String) -> Self {
        Op::DEL { timestamp, key }
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        BytecodeSerializer::op_to_bytes(&self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OpType {
    SET,
    GET,
    DEL,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OpBuilder {
    timestamp: i64,
    op_type: Option<OpType>,
    key: Option<String>,
    value: Option<String>,
}

impl OpBuilder {
    pub fn new() -> Self {
        let timestamp = 0i64;
        Self {
            timestamp,
            op_type: None,
            key: None,
            value: None,
        }
    }

    pub fn set_op_type(&mut self, op_type: OpType) -> &mut Self {
        self.op_type = Some(op_type);
        self
    }

    pub fn set_key(&mut self, key: String) -> &mut Self {
        self.key = Some(key);
        self
    }

    pub fn set_value(&mut self, value: String) -> &mut Self {
        self.value = Some(value);
        self
    }

    pub fn build(&self) -> Option<Op> {
        match self.op_type {
            Some(OpType::SET) => match (&self.key, &self.value) {
                (Some(k), Some(v)) => Some(Op::new_set(self.timestamp, k.clone(), v.clone())),
                _ => None,
            },
            Some(OpType::GET) => match &self.key {
                Some(k) => Some(Op::new_get(self.timestamp, k.clone())),
                _ => None,
            },
            Some(OpType::DEL) => match &self.key {
                Some(k) => Some(Op::new_del(self.timestamp, k.clone())),
                _ => None,
            },
            _ => None,
        }
    }
}
