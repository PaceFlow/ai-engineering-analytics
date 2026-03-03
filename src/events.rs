/// All events that can be ingested from providers.
pub enum Event {
    ChatStart {
        session_id: String,
        provider: String,
        project_path: Option<String>,
        timestamp: Option<String>,
        last_updated: Option<String>,
    },
    Message {
        session_id: String,
        provider: String,
        role: String,
        content: String,
        content_words: usize,
        timestamp: Option<String>,
    },
    ChangesAccepted {
        session_id: String,
        provider: String,
        file_path: String,
        lines_added: i64,
        lines_removed: i64,
        timestamp: Option<String>,
    },
}
