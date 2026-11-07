use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]

pub struct CompiledScripts {
    pub modules: HashMap<String, Vec<u8>>,
    pub fingerprint: String,
}
