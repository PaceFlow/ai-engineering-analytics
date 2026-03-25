#!/usr/bin/env python3

import json
import re
import sqlite3
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "regression" / "home_template"
CODEX_ROOT = FIXTURE_ROOT / ".codex" / "sessions"
CURSOR_DB = FIXTURE_ROOT / "cursor" / "state.vscdb"

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
DOC_TITLE_RE = re.compile(
    r"(How Developers Fight Vibe Coding Problems|Vibe Coding KPIs [^\"]+?ROI)\.pdf"
)

USER_TEMPLATES = [
    "Please review the implementation and keep the behavior stable.",
    "Can you tighten this logic without changing the user-visible output?",
    "Please simplify the code path and preserve the current behavior.",
    "Can you investigate the current implementation and summarize the safest fix?",
]

ASSISTANT_TEMPLATES = [
    "Reviewed the request, preserved the behavior, and summarized the implementation tradeoffs.",
    "Inspected the code path, kept behavior stable, and outlined the safest update.",
    "Checked the implementation, preserved the current behavior, and noted the key follow-ups.",
]

CONTINUATION_TEMPLATES = [
    "same error",
    "still failing",
    "doesnt work, same error",
]

ERROR_TEMPLATES = [
    "TypeError: synthetic regression failure at line 12",
    "RuntimeError: synthetic fixture build failed",
    "test failed: synthetic regression case",
]

EMPTY_RICH_TEXT = json.dumps(
    {
        "root": {
            "children": [
                {
                    "children": [],
                    "format": "",
                    "indent": 0,
                    "type": "paragraph",
                    "version": 1,
                }
            ],
            "format": "",
            "indent": 0,
            "type": "root",
            "version": 1,
        }
    },
    separators=(",", ":"),
)


def contains_error_signal(text: str) -> bool:
    lower = text.lower()
    markers = [
        "traceback",
        "typeerror",
        "referenceerror",
        "syntaxerror",
        "runtimeerror",
        "panic:",
        "exception:",
        "stack trace",
        "error:",
        "build failed",
        "test failed",
        "tests failed",
        "compilation failed",
        "cannot find module",
        "module not found",
    ]
    if any(marker in lower for marker in markers):
        return True
    return bool(re.search(r"\b\d+\s+errors?\b", lower))


def is_error_continuation(text: str) -> bool:
    lower = text.lower()
    phrases = [
        "same error",
        "same issue",
        "still failing",
        "still fails",
        "still broken",
        "not fixed",
        "didnt work",
        "doesnt work",
        "still not working",
    ]
    return any(phrase in lower for phrase in phrases)


def sanitize_tool_output(text: str) -> str:
    cleaned = EMAIL_RE.sub("redacted@example.com", text)
    cleaned = re.sub(
        r"Author:\s+.*",
        "Contributor: Sanitized <redacted@example.com>",
        cleaned,
    )
    if "|codex|user|" in cleaned or "|codex|assistant|" in cleaned:
        return "Sanitized fixture output.\n"
    if "__HOME__/Downloads/" in cleaned and ".pdf" in cleaned:
        return "Sanitized external reference output.\n"
    if "%PDF-" in cleaned:
        return "Sanitized external reference output.\n"
    if cleaned.startswith("commit ") and "diff --git" in cleaned:
        return "Sanitized git inspection output.\n"
    if "Vibe Coding KPIs" in cleaned or "=== PAGE 1 ===" in cleaned:
        return "Sanitized external reference output.\n"
    return cleaned


def sanitize_command_arguments(arguments: str) -> str:
    if "Vibe Coding KPIs" not in arguments and "How Developers Fight Vibe Coding Problems" not in arguments:
        return arguments
    return DOC_TITLE_RE.sub("sanitized-reference.pdf", arguments)


class MessageSanitizer:
    def __init__(self) -> None:
        self.user_index = 0
        self.assistant_index = 0

    def user_text(self, text: str) -> str:
        self.user_index += 1
        if is_error_continuation(text):
            return CONTINUATION_TEMPLATES[(self.user_index - 1) % len(CONTINUATION_TEMPLATES)]
        if contains_error_signal(text):
            return ERROR_TEMPLATES[(self.user_index - 1) % len(ERROR_TEMPLATES)]
        if EMAIL_RE.search(text) or "author:" in text.lower():
            return "Please adjust the commit matching logic without changing the surrounding behavior."
        return USER_TEMPLATES[(self.user_index - 1) % len(USER_TEMPLATES)]

    def assistant_text(self, _text: str) -> str:
        self.assistant_index += 1
        return ASSISTANT_TEMPLATES[(self.assistant_index - 1) % len(ASSISTANT_TEMPLATES)]

    def by_role(self, role: str, text: str) -> str:
        if role == "user":
            return self.user_text(text)
        if role == "assistant":
            return self.assistant_text(text)
        return text


def sanitize_content_items(items, role: str, sanitizer: MessageSanitizer) -> None:
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type in {"input_text", "output_text", "summary_text"} and isinstance(
            item.get("text"), str
        ):
            item["text"] = sanitizer.by_role(role, item["text"])


def sanitize_compacted_history(payload: dict, sanitizer: MessageSanitizer) -> None:
    history = payload.get("replacement_history")
    if not isinstance(history, list):
        return
    for entry in history:
        if not isinstance(entry, dict):
            continue
        role = entry.get("role")
        if role not in {"user", "assistant"}:
            continue
        sanitize_content_items(entry.get("content"), role, sanitizer)


def sanitize_codex_session(path: Path) -> None:
    sanitizer = MessageSanitizer()
    new_lines = []
    for raw_line in path.read_text().splitlines():
        if not raw_line.strip():
            new_lines.append(raw_line)
            continue
        record = json.loads(raw_line)
        record_type = record.get("type")
        payload = record.get("payload")

        if record_type == "session_meta" and isinstance(payload, dict):
            base_instructions = payload.get("base_instructions")
            if isinstance(base_instructions, dict) and isinstance(base_instructions.get("text"), str):
                base_instructions["text"] = (
                    "Synthetic regression fixture instructions. Original fixture content was anonymized."
                )
        elif record_type == "event_msg" and isinstance(payload, dict):
            payload_type = payload.get("type")
            if payload_type == "user_message" and isinstance(payload.get("message"), str):
                payload["message"] = sanitizer.user_text(payload["message"])
            elif payload_type == "agent_message" and isinstance(payload.get("message"), str):
                payload["message"] = sanitizer.assistant_text(payload["message"])
            elif payload_type == "agent_reasoning" and isinstance(payload.get("text"), str):
                payload["text"] = "Summarizing the safest implementation approach."
            elif payload_type == "task_complete" and isinstance(payload.get("last_agent_message"), str):
                payload["last_agent_message"] = sanitizer.assistant_text(
                    payload["last_agent_message"]
                )
        elif record_type == "response_item" and isinstance(payload, dict):
            payload_type = payload.get("type")
            role = payload.get("role")
            if payload_type == "message" and role in {"user", "assistant"}:
                sanitize_content_items(payload.get("content"), role, sanitizer)
            elif payload_type == "reasoning":
                sanitize_content_items(payload.get("summary"), "assistant", sanitizer)
                payload["encrypted_content"] = None
            elif payload_type == "function_call" and isinstance(payload.get("arguments"), str):
                payload["arguments"] = sanitize_command_arguments(payload["arguments"])
            elif payload_type == "function_call_output" and isinstance(payload.get("output"), str):
                payload["output"] = sanitize_tool_output(payload["output"])
        elif record_type == "compacted" and isinstance(payload, dict):
            sanitize_compacted_history(payload, sanitizer)

        new_lines.append(json.dumps(record, separators=(",", ":")))

    path.write_text("\n".join(new_lines) + "\n")


def sanitize_cursor_row(key: str, value: str, row_index: int) -> str:
    parsed = json.loads(value)
    if key.startswith("bubbleId:"):
        if isinstance(parsed, dict) and isinstance(parsed.get("text"), str):
            parsed["text"] = USER_TEMPLATES[row_index % len(USER_TEMPLATES)]
        return json.dumps(parsed, separators=(",", ":"))

    if key.startswith("composerData:") and isinstance(parsed, dict):
        if isinstance(parsed.get("text"), str):
            parsed["text"] = USER_TEMPLATES[row_index % len(USER_TEMPLATES)]
        if isinstance(parsed.get("richText"), str):
            parsed["richText"] = EMPTY_RICH_TEXT
        conversation = parsed.get("conversation")
        if isinstance(conversation, list):
            for idx, bubble in enumerate(conversation):
                if not isinstance(bubble, dict) or not isinstance(bubble.get("text"), str):
                    continue
                if bubble.get("bubbleType") == 1:
                    bubble["text"] = USER_TEMPLATES[idx % len(USER_TEMPLATES)]
                else:
                    bubble["text"] = ASSISTANT_TEMPLATES[idx % len(ASSISTANT_TEMPLATES)]
        return json.dumps(parsed, separators=(",", ":"))

    return value


def sanitize_cursor_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute("SELECT key, value FROM cursorDiskKV ORDER BY key").fetchall()
        for idx, (key, value) in enumerate(rows):
            updated = sanitize_cursor_row(key, value, idx)
            if updated != value:
                conn.execute(
                    "UPDATE cursorDiskKV SET value = ?1 WHERE key = ?2",
                    (updated, key),
                )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    for path in sorted(CODEX_ROOT.rglob("*.jsonl")):
        sanitize_codex_session(path)
    sanitize_cursor_db(CURSOR_DB)


if __name__ == "__main__":
    main()
