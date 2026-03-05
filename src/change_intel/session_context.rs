use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SessionContext {
    pub session_cwd: Option<String>,
    pub file_cache: HashMap<String, String>,
}

impl SessionContext {
    pub fn new(session_cwd: Option<String>) -> Self {
        Self {
            session_cwd,
            file_cache: HashMap::new(),
        }
    }
}
