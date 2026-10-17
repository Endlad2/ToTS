//! Новоtype-обёртки для токенов.

#[derive(Debug, Clone)]
pub struct AnonToken(pub String);

#[derive(Debug, Clone)]
pub struct CallToken(pub String);

#[derive(Debug, Clone)]
pub struct OkSession(pub String);
