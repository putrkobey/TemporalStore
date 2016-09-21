// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::io::{self, BufRead, Write};

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.write_to(&mut out).expect("vec write cannot fail");
        out
    }

    fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        match self {
            RespValue::SimpleString(value) => write!(writer, "+{value}\r\n"),
            RespValue::Error(value) => write!(writer, "-{value}\r\n"),
            RespValue::Integer(value) => write!(writer, ":{value}\r\n"),
            RespValue::Bulk(Some(value)) => {
                write!(writer, "${}\r\n", value.len())?;
                writer.write_all(value)?;
                writer.write_all(b"\r\n")
            }
            RespValue::Bulk(None) => writer.write_all(b"$-1\r\n"),
            RespValue::Array(values) => {
                write!(writer, "*{}\r\n", values.len())?;
                for value in values {
                    value.write_to(writer)?;
                }
                Ok(())
            }
        }
    }
}

pub fn read_command(reader: &mut impl BufRead) -> io::Result<Option<Vec<Vec<u8>>>> {
    let mut first = Vec::new();
    let bytes = reader.read_until(b'\n', &mut first)?;
    if bytes == 0 {
        return Ok(None);
    }
    trim_crlf(&mut first);
    if first.is_empty() {
        return Ok(Some(Vec::new()));
