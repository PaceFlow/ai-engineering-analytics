use crate::change_intel::session_context::SessionContext;

#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub provider: String,
    pub session_id: String,
    pub source_file: String,
    pub session_cwd: Option<String>,
    pub last_seen_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ToolCallEvent {
    pub provider: String,
    pub session_id: String,
    pub source_file: String,
    pub timestamp: Option<String>,
    pub call_id: String,
    pub tool_name: String,
    pub input_json: String,
    pub output_json: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Overwrite,
    Append,
    Patch,
}

impl WriteMode {
    pub fn as_str(self) -> &'static str {
        match self {
            WriteMode::Overwrite => "overwrite",
            WriteMode::Append => "append",
            WriteMode::Patch => "patch",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LineSide {
    Added,
    Removed,
}

impl LineSide {
    pub fn as_str(self) -> &'static str {
        match self {
            LineSide::Added => "+",
            LineSide::Removed => "-",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LineHashCount {
    pub side: LineSide,
    pub line_hash: String,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct ChangeOpCandidate {
    pub provider: String,
    pub session_id: String,
    pub call_id: String,
    pub op_index: i32,
    pub timestamp: Option<String>,
    pub repo_root: Option<String>,
    pub abs_path: String,
    pub rel_path: Option<String>,
    pub write_mode: WriteMode,
    pub before_known: bool,
    pub added_lines: i64,
    pub removed_lines: i64,
    pub parser_name: String,
    pub line_hashes: Vec<LineHashCount>,
}

#[derive(Debug, Clone)]
pub struct ParseError {
    pub provider: String,
    pub session_id: String,
    pub source_file: String,
    pub call_id: String,
    pub timestamp: Option<String>,
    pub parser_name: String,
    pub error: String,
}

#[derive(Debug, Clone, Default)]
pub struct ParseOutcome {
    pub ops: Vec<ChangeOpCandidate>,
    pub errors: Vec<ParseError>,
}

pub trait PatternParser {
    fn name(&self) -> &'static str;
    fn parse(&self, event: &ToolCallEvent, ctx: &mut SessionContext) -> ParseOutcome;
}
