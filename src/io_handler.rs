use std::collections::VecDeque;
use tokio::io::AsyncBufReadExt;

use crate::{errors::ParserError, operation::Op, parser::Parser};

pub struct IOHandler {
    parser: Parser,
    operation_queue: VecDeque<Op>,
}

impl IOHandler {
    pub fn new() -> Self {
        let parser = Parser::new();
        let operation_queue = VecDeque::new();
        Self { parser }
    }

    pub async fn parse_input<R: AsyncBufReadExt + Unpin>(
        &mut self,
        input: R,
    ) -> Result<Vec<Op>, ParserError> {
        let op = self.parser.parse(input).await?;
        Ok(op)
    }
}
