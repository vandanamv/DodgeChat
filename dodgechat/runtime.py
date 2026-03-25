import json
import hashlib
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .config import CACHE_VERSION, DEFAULT_MODEL, OPENROUTER_BASE_URL, RELATED_TABLE_HINTS, SQL_ROW_LIMIT
from .paths import BASE_DIR, CACHE_DIR, DATASET_DIR, GRAPH_CACHE_PATH, SQLITE_CACHE_PATH

def link_signature(link: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        str(link["source_table"]),
        str(link["source_field"]),
        str(link["target_table"]),
        str(link["target_field"]),
    )


def compute_dataset_signature() -> str:
    digest = hashlib.sha256()
    digest.update(str(CACHE_VERSION).encode("utf-8"))
    for path in sorted(DATASET_DIR.rglob("*.jsonl")):
        stat = path.stat()
        digest.update(str(path.relative_to(DATASET_DIR)).encode("utf-8"))
        digest.update(str(stat.st_size).encode("utf-8"))
        digest.update(str(stat.st_mtime_ns).encode("utf-8"))
    return digest.hexdigest()


def load_cached_graph_state(dataset_signature: str) -> Optional[Dict[str, Any]]:
    if not GRAPH_CACHE_PATH.exists() or not SQLITE_CACHE_PATH.exists():
        return None
    try:
        payload = json.loads(GRAPH_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("cacheVersion") != CACHE_VERSION:
        return None
    if payload.get("datasetSignature") != dataset_signature:
        return None
    state_payload = payload.get("state")
    return state_payload if isinstance(state_payload, dict) else None


def save_cached_graph_state(dataset_signature: str, state_payload: Dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    GRAPH_CACHE_PATH.write_text(
        json.dumps(
            {
                "cacheVersion": CACHE_VERSION,
                "datasetSignature": dataset_signature,
                "state": state_payload,
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )


def load_dotenv(dotenv_path: Path = BASE_DIR / ".env") -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def norm_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return re.sub(r"^0+(?=\d)", "", text).lower()


def identifier_tokens_from_value(value: Any) -> List[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    matches = re.findall(r"\b[A-Z0-9][A-Z0-9_-]{4,}\b|\b\d{5,}\b", text, re.IGNORECASE)
    tokens: List[str] = []
    seen = set()
    direct = norm_id(text)
    if direct:
        tokens.append(direct)
        seen.add(direct)
    for match in matches:
        token = norm_id(match)
        if token and token not in seen:
            seen.add(token)
            tokens.append(token)
    return tokens


def related_tables_for_column(column_name: str) -> Tuple[str, ...]:
    normalized = re.sub(r"[^a-z0-9]+", "", str(column_name or "").lower())
    if not normalized:
        return ()
    return RELATED_TABLE_HINTS.get(normalized, ())


def json_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    if value is None:
        return None
    return str(value)


def node_key(row: Dict[str, Any], fields: Sequence[str]) -> Optional[str]:
    parts = []
    for field in fields:
        value = row.get(field)
        if value in (None, ""):
            return None
        parts.append(norm_id(value))
    return "|".join(parts)


def build_label(row: Dict[str, Any], fields: Sequence[str], fallback: str) -> str:
    values = [str(row.get(field) or "").strip() for field in fields]
    values = [value for value in values if value]
    return " / ".join(values) if values else fallback


def call_openrouter(messages: List[Dict[str, str]], max_tokens: int) -> str:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set. Add it to .env.")

    base_url = os.getenv("OPENROUTER_BASE_URL", OPENROUTER_BASE_URL).rstrip("/")
    model = os.getenv("OPENAI_MODEL", DEFAULT_MODEL)
    payload = {"model": model, "max_tokens": max_tokens, "temperature": 0.1, "messages": messages}
    request = urllib.request.Request(
        url=f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://local.dataset.graph",
            "X-Title": "DodgeChat Graph",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenRouter request failed ({exc.code}): {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach OpenRouter: {exc.reason}") from exc

    content = body.get("choices", [{}])[0].get("message", {}).get("content", "")
    if isinstance(content, list):
        return "".join(part.get("text", "") for part in content if isinstance(part, dict)).strip()
    return str(content).strip()


def extract_json_object(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"Expected JSON object, got: {text[:200]}")
    return json.loads(text[start:end + 1])


def safe_sql(sql: str) -> str:
    sql = sql.strip().rstrip(";")
    compact = re.sub(r"\s+", " ", sql).strip().lower()
    if not compact.startswith("select") and not compact.startswith("with"):
        raise ValueError("Only SELECT statements are allowed.")
    banned = [" insert ", " update ", " delete ", " drop ", " alter ", " pragma ", " attach ", " detach ", " create ", " replace ", " vacuum ", " truncate "]
    wrapped = f" {compact} "
    if any(token in wrapped for token in banned):
        raise ValueError("SQL contains a disallowed statement.")
    if ";" in sql:
        raise ValueError("Only one SQL statement is allowed.")
    if " limit " not in wrapped:
        sql = f"{sql}\nLIMIT {SQL_ROW_LIMIT}"
    return sql
