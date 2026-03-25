import argparse
import json
import os
import re
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


DEFAULT_DATASET_DIR = Path("sap-order-to-cash-dataset") / "sap-o2c-data"
DEFAULT_MODEL = "openai/gpt-4.1-mini"
DEFAULT_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_MAX_TOKENS = 1200
TOKEN_RE = re.compile(r"[A-Za-z0-9_./-]+")
IDENTIFIER_RE = re.compile(r"\b[A-Z0-9][A-Z0-9_-]{3,}\b|\b\d{4,}\b", re.IGNORECASE)
MAX_CONTEXT_RECORDS = 18
MAX_RECORD_CHARS = 900


@dataclass
class Record:
    table: str
    source_file: str
    line_number: int
    data: Dict[str, object]
    searchable_text: str
    compact_json: str


def normalize_token(token: str) -> str:
    return token.lower().strip()


def tokenize(text: str) -> List[str]:
    return [normalize_token(match.group(0)) for match in TOKEN_RE.finditer(text)]


def serialize_row(row: Dict[str, object]) -> str:
    return json.dumps(row, ensure_ascii=True, separators=(",", ":"), default=str)


from dodgechat.runtime import load_dotenv


class DatasetIndex:
    def __init__(self, dataset_dir: Path) -> None:
        self.dataset_dir = dataset_dir
        self.records: List[Record] = []
        self.table_names: List[str] = []
        self.identifier_to_records: Dict[str, List[int]] = defaultdict(list)
        self._load()

    def _load(self) -> None:
        if not self.dataset_dir.exists():
            raise FileNotFoundError(f"Dataset directory not found: {self.dataset_dir}")

        for table_dir in sorted(path for path in self.dataset_dir.iterdir() if path.is_dir()):
            self.table_names.append(table_dir.name)
            for jsonl_path in sorted(table_dir.glob("*.jsonl")):
                with jsonl_path.open("r", encoding="utf-8") as handle:
                    for line_number, raw_line in enumerate(handle, start=1):
                        row = json.loads(raw_line)
                        compact_json = serialize_row(row)
                        searchable_parts = [table_dir.name]
                        searchable_parts.extend(f"{key} {value}" for key, value in row.items())
                        searchable_text = " ".join(searchable_parts).lower()
                        record = Record(
                            table=table_dir.name,
                            source_file=str(jsonl_path.relative_to(self.dataset_dir.parent)),
                            line_number=line_number,
                            data=row,
                            searchable_text=searchable_text,
                            compact_json=compact_json,
                        )
                        record_index = len(self.records)
                        self.records.append(record)
                        for value in row.values():
                            if isinstance(value, (str, int, float)):
                                token = normalize_token(str(value))
                                if 4 <= len(token) <= 40:
                                    self.identifier_to_records[token].append(record_index)

    def search(self, question: str, limit: int = MAX_CONTEXT_RECORDS) -> List[Tuple[float, Record]]:
        question_tokens = tokenize(question)
        question_token_set = set(question_tokens)
        identifier_hits = {
            normalize_token(match.group(0))
            for match in IDENTIFIER_RE.finditer(question)
        }
        table_mentions = {
            table for table in self.table_names if table.replace("_", " ") in question.lower() or table in question.lower()
        }

        scored: Dict[int, float] = defaultdict(float)
        exact_match_indexes = set()

        for identifier in identifier_hits:
            for record_index in self.identifier_to_records.get(identifier, []):
                exact_match_indexes.add(record_index)
                scored[record_index] += 30.0

        if identifier_hits and not exact_match_indexes:
            return []

        candidate_indexes: Iterable[int]
        if exact_match_indexes:
            candidate_indexes = exact_match_indexes
        else:
            candidate_indexes = range(len(self.records))

        for record_index in candidate_indexes:
            record = self.records[record_index]
            if table_mentions and record.table in table_mentions:
                scored[record_index] += 6.0

            overlap = 0
            for token in question_token_set:
                if len(token) < 3:
                    continue
                if token in record.searchable_text:
                    overlap += 1
            if overlap:
                scored[record_index] += overlap

        ranked = sorted(
            ((score, self.records[index]) for index, score in scored.items() if score > 0),
            key=lambda item: item[0],
            reverse=True,
        )
        return ranked[:limit]


def build_context(question: str, matches: Sequence[Tuple[float, Record]]) -> str:
    if not matches:
        return "No matching records were found in the dataset."

    grouped: Dict[str, List[str]] = defaultdict(list)
    for score, record in matches:
        snippet = record.compact_json[:MAX_RECORD_CHARS]
        grouped[record.table].append(
            f"- score={score:.1f}, source={record.source_file}:{record.line_number}, row={snippet}"
        )

    lines = [f"User question: {question}", "", "Relevant dataset records:"]
    for table_name in sorted(grouped):
        lines.append(f"[{table_name}]")
        lines.extend(grouped[table_name])
        lines.append("")
    return "\n".join(lines).strip()


def ask_llm(question: str, context: str, model: str, max_tokens: int) -> str:
    base_url = os.getenv("OPENROUTER_BASE_URL", DEFAULT_OPENROUTER_BASE_URL).rstrip("/")
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You answer questions only using the supplied SAP order-to-cash dataset context. "
                    "If the context is incomplete, say that clearly. "
                    "Cite key identifiers such as sales orders, billing documents, customers, or products when available."
                ),
            },
            {
                "role": "user",
                "content": f"{context}\n\nAnswer the user's question in a concise, helpful way.",
            },
        ],
    }
    request = urllib.request.Request(
        url=f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://local.dataset.qa",
            "X-Title": "Dataset Q&A",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            response_data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"OpenRouter request failed ({exc.code}): {error_body}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Could not reach OpenRouter: {exc.reason}") from exc

    choices = response_data.get("choices", [])
    if not choices:
        raise SystemExit(f"OpenRouter returned no choices: {response_data}")

    message = choices[0].get("message", {})
    content = message.get("content", "")
    if isinstance(content, list):
        text_parts = [part.get("text", "") for part in content if isinstance(part, dict)]
        return "".join(text_parts).strip()
    return str(content).strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ask questions about the SAP order-to-cash dataset and answer with one LLM response."
    )
    parser.add_argument("question", nargs="*", help="Natural-language question about the dataset.")
    parser.add_argument(
        "--dataset-dir",
        default=str(DEFAULT_DATASET_DIR),
        help="Path to the dataset root directory.",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENAI_MODEL", DEFAULT_MODEL),
        help="OpenAI model to use for the final answer.",
    )
    parser.add_argument(
        "--show-context",
        action="store_true",
        help="Print the retrieved dataset context before the LLM answer.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=int(os.getenv("OPENROUTER_MAX_TOKENS", str(DEFAULT_MAX_TOKENS))),
        help="Maximum tokens to generate in the final answer.",
    )
    return parser.parse_args()


def resolve_question(args: argparse.Namespace) -> str:
    if args.question:
        return " ".join(args.question).strip()
    return input("Ask a question about the dataset: ").strip()


def main() -> None:
    load_dotenv()
    args = parse_args()
    question = resolve_question(args)
    if not question:
        raise SystemExit("Please provide a question.")

    if not os.getenv("OPENROUTER_API_KEY"):
        raise SystemExit("OPENROUTER_API_KEY is not set. Add it to .env.")

    dataset_index = DatasetIndex(Path(args.dataset_dir))
    matches = dataset_index.search(question)
    context = build_context(question, matches)

    if args.show_context:
        print("=== Retrieved Context ===")
        print(context)
        print()

    answer = ask_llm(question, context, args.model, args.max_tokens)
    print(answer)


if __name__ == "__main__":
    main()
