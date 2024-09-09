use crate::{
    errors::MemoryLayerErrors,
    operation::{Op, OpBuilder, OpType},
};
use core::str;
use std::error::Error;
use tokio::io::AsyncBufReadExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Token {
    SET,
    GET,
    DEL,
    AND,
    TO,
    LITERAL(String),
    EOF,
}

struct Lexer;

impl Lexer {
    pub fn new() -> Self {
        Self {}
    }

    async fn tokenize<R: AsyncBufReadExt + Unpin>(
        &self,
        mut buffer: R,
    ) -> Result<Vec<Token>, Box<dyn Error>> {
        let mut bytes: Vec<u8> = vec![];
        let mut tokens: Vec<Token> = vec![];

        // Read data until newline
        buffer.read_until(b'\n', &mut bytes).await?;
        let input = str::from_utf8(&bytes)?;

        for word in input.split_whitespace() {
            let token = self.eval_word(word);
            tokens.push(token);
        }

        Ok(tokens)
    }

    fn eval_word(&self, word: &str) -> Token {
        // Use get_or_insert_with to avoid unnecessary clones
        match word {
            "SET" => Token::SET,
            "GET" => Token::GET,
            "DEL" => Token::DEL,
            "AND" => Token::AND,
            "TO" => Token::TO,
            _ => Token::LITERAL(word.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParserStates {
    Start,
    Set,
    Get,
    Del,
    To,
    Key,
    Value,
}

pub struct StateMachine {
    state: ParserStates,
    op_builder: OpBuilder,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            state: ParserStates::Start,
            op_builder: OpBuilder::new(),
        }
    }

    pub fn process(&mut self, token: Token) -> Result<(), MemoryLayerErrors> {
        match &self.state {
            ParserStates::Start => match token {
                Token::SET => {
                    self.op_builder.set_op_type(OpType::SET);
                    self.state = ParserStates::Set;
                    Ok(())
                }
                Token::GET => {
                    self.op_builder.set_op_type(OpType::GET);
                    self.state = ParserStates::Get;
                    Ok(())
                }
                Token::DEL => {
                    self.op_builder.set_op_type(OpType::DEL);
                    self.state = ParserStates::Del;
                    Ok(())
                }
                _ => Err(MemoryLayerErrors::GenericError(
                    "Invalid operation".to_string(),
                )),
            },

            ParserStates::Key => match token {
                Token::TO => {
                    self.state = ParserStates::To;
                    Ok(())
                }
                Token::AND => {
                    self.op_builder = OpBuilder::new();
                    self.state = ParserStates::Start;
                    Ok(())
                }
                Token::EOF => {
                    self.state = ParserStates::Start;
                    Ok(())
                }
                _ => Err(MemoryLayerErrors::GenericError(
                    "Invalid operation".to_string(),
                )),
            },
            ParserStates::To => match token {
                Token::LITERAL(value) => {
                    self.op_builder.set_value(value.clone());
                    self.state = ParserStates::Value;
                    Ok(())
                }
                _ => Err(MemoryLayerErrors::GenericError("Invalid value".to_string())),
            },
            ParserStates::Value => match token {
                Token::AND => {
                    self.op_builder = OpBuilder::new();
                    self.state = ParserStates::Start;
                    Ok(())
                }
                Token::EOF => {
                    self.state = ParserStates::Start;
                    Ok(())
                }
                _ => Err(MemoryLayerErrors::GenericError(
                    "Invalid operation".to_string(),
                )),
            },
            ParserStates::Set | ParserStates::Get | ParserStates::Del => {
                if let Token::LITERAL(ref key) = token {
                    self.op_builder.set_key(key.clone());
                    self.state = ParserStates::Key;
                    Ok(())
                } else {
                    return Err(MemoryLayerErrors::GenericError("Invalid key".to_string()));
                }
            }
        }
    }

    pub fn get_operation(&mut self) -> Option<Op> {
        self.op_builder.build()
    }
}

pub struct Parser {
    token_stream: Vec<Token>,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            token_stream: Vec::new(),
        }
    }

    pub async fn parse<R: AsyncBufReadExt + Unpin>(
        &mut self,
        buffer: R,
    ) -> Result<Vec<Op>, Box<dyn Error>> {
        let lexer = Lexer::new();
        self.token_stream = lexer.tokenize(buffer).await?;
        let mut operations: Vec<Op> = vec![];
        let mut state_machine = StateMachine::new();
        let mut token_iter = self.token_stream.iter().peekable();
        while let Some(token) = token_iter.next() {
            state_machine.process(token.clone())?;
            if let Some(op) = state_machine.get_operation() {
                operations.push(op);
            }
        }
        if operations.is_empty() {
            return Err(Box::new(MemoryLayerErrors::GenericError(
                "No valid operations found".to_string(),
            )));
        }
        Ok(operations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_lexer() {
        let lexer = Lexer::new();
        let buffer = r#"SET key1 TO value1 AND GET key1 AND DEL key1 AND SET key2 TO value2 AND GET key2 AND DEL key2"#;
        let mut reader = BufReader::new(buffer.as_bytes());
        let tokens = lexer.tokenize(&mut reader).await.unwrap();
        let expected = vec![
            Token::SET,
            Token::LITERAL("key1".to_string()),
            Token::TO,
            Token::LITERAL("value1".to_string()),
            Token::AND,
            Token::GET,
            Token::LITERAL("key1".to_string()),
            Token::AND,
            Token::DEL,
            Token::LITERAL("key1".to_string()),
            Token::AND,
            Token::SET,
            Token::LITERAL("key2".to_string()),
            Token::TO,
            Token::LITERAL("value2".to_string()),
            Token::AND,
            Token::GET,
            Token::LITERAL("key2".to_string()),
            Token::AND,
            Token::DEL,
            Token::LITERAL("key2".to_string()),
        ];
        assert_eq!(tokens, expected);
    }

    #[tokio::test]
    async fn test_parser() {
        let mut parser = Parser::new();
        let buffer = r#"SET key1 TO value1 AND GET key1 AND DEL key1 AND SET key2 TO value2 AND GET key2 AND DEL key2"#;
        let mut reader = BufReader::new(buffer.as_bytes());
        let operations = parser.parse(&mut reader).await.unwrap();
        let expected = vec![
            Op::new_set(0, "key1".to_string(), "value1".to_string()),
            Op::new_get(0, "key1".to_string()),
            Op::new_del(0, "key1".to_string()),
            Op::new_set(0, "key2".to_string(), "value2".to_string()),
            Op::new_get(0, "key2".to_string()),
            Op::new_del(0, "key2".to_string()),
        ];
        assert_eq!(operations, expected);
    }

    #[tokio::test]
    async fn test_parser_error() {
        let mut parser = Parser::new();
        let buffer = r#"SET key1 DEL key2 "#;
        let mut reader = BufReader::new(buffer.as_bytes());
        let result = parser.parse(&mut reader).await;
        assert!(result.is_err());
    }
}
