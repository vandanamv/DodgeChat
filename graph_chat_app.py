import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from dodgechat.paths import FRONTEND_DIR
from dodgechat.questions import (
    billing_status_answer,
    billing_status_rows,
    build_deterministic_chat_result,
    build_focus_context,
    customer_billing_answer,
    customer_billing_rows,
    customer_sales_order_answer,
    customer_sales_order_rows,
    expand_related_node_ids_with_paths,
    full_flow_answer,
    full_flow_rows,
    generate_and_execute_sql,
    graph_context_answer,
    graph_context_rows,
    graph_relation_fallback,
    is_billing_status_question,
    is_customer_billing_question,
    is_customer_sales_orders_question,
    is_dataset_domain_question,
    is_full_flow_question,
    is_graph_neighborhood_question,
    is_list_intent,
    llm_answer,
    related_node_ids_from_rows,
    rows_to_list_answer,
    resolve_focus_node,
)
from dodgechat.runtime import load_dotenv
from dodgechat.state import AppState, init_state
from dodgechat.config import DEFAULT_HOST, DEFAULT_PORT


BASE_DIR = Path(__file__).resolve().parent
STATIC_ROOTS = []
for candidate in [FRONTEND_DIR, BASE_DIR / "Frontend"]:

    resolved = candidate.resolve()
    if resolved.exists() and resolved not in STATIC_ROOTS:
        STATIC_ROOTS.append(resolved)


def resolve_static_path(relative_path: str) -> Optional[Path]:
    normalized = relative_path.lstrip("/")
    if not normalized:
        normalized = "index.html"
    for root in STATIC_ROOTS:
        candidate = (root / normalized).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            continue
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def mime_type_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".html":
        return "text/html; charset=utf-8"
    if suffix == ".css":
        return "text/css; charset=utf-8"
    if suffix == ".js":
        return "application/javascript; charset=utf-8"
    return "text/plain; charset=utf-8"


class GraphChatHandler(BaseHTTPRequestHandler):
    state: AppState = None  # type: ignore[assignment]

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, relative_path: str) -> None:
        path = resolve_static_path(relative_path)
        if not path:
            self._send_json(404, {"error": "Not found"})
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime_type_for(path))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _chat_response(
        self,
        answer: str,
        sql: str,
        rows: Sequence[Dict[str, Any]],
        columns: Sequence[str],
        related_node_ids: Sequence[str],
        notes: str,
        resolved_focus_node_id: Optional[str],
    ) -> Dict[str, Any]:
        return {
            "answer": answer,
            "sql": sql,
            "rows": list(rows),
            "columns": list(columns),
            "relatedNodeIds": list(related_node_ids),
            "notes": notes,
            "resolvedFocusNodeId": resolved_focus_node_id,
        }

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in {"/", "/index.html"}:
            self._serve_static("index.html")
            return
        if parsed.path == "/api/health":
            self._send_json(200, {"ok": True})
            return
        if parsed.path == "/api/graph":
            self._send_json(200, self.state.ui_graph_payload)
            return
        if parsed.path.startswith("/"):
            relative_path = parsed.path.lstrip("/")
            if resolve_static_path(relative_path):
                self._serve_static(relative_path)
                return
        self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/api/chat":
            self._send_json(404, {"error": "Not found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            question = str(payload.get("question", "")).strip()
            if not question:
                self._send_json(400, {"error": "Question is required."})
                return
            history = payload.get("history", [])
            if not isinstance(history, list):
                history = []
            focus_node_id = payload.get("focusNodeId")
            resolved_focus_node_id = resolve_focus_node(self.state, question, focus_node_id)
            focus_node = build_focus_context(self.state, resolved_focus_node_id)

            if not is_dataset_domain_question(self.state, question, focus_node):
                self._send_json(
                    200,
                    self._chat_response(
                        answer="This system is designed to answer questions related to the provided dataset only.",
                        sql="-- rejected by dataset guardrail",
                        rows=[],
                        columns=[],
                        related_node_ids=[],
                        notes="Rejected unrelated prompt outside the dataset domain.",
                        resolved_focus_node_id=resolved_focus_node_id,
                    ),
                )
                return

            if is_graph_neighborhood_question(question, focus_node):
                rows = graph_context_rows(self.state, focus_node)
                result = self._chat_response(
                    answer=graph_context_answer(question, focus_node, rows),
                    sql="-- graph-context answer; no SQL executed",
                    rows=rows,
                    columns=sorted({key for row in rows for key in row.keys()}),
                    related_node_ids=expand_related_node_ids_with_paths(
                        self.state,
                        [resolved_focus_node_id] + self.state.adjacency.get(resolved_focus_node_id, []),
                        resolved_focus_node_id,
                    ),
                    notes="Answered directly from inferred graph context.",
                    resolved_focus_node_id=resolved_focus_node_id,
                )
            elif is_billing_status_question(question):
                rows = billing_status_rows(self.state, focus_node, question)
                result = self._chat_response(
                    **build_deterministic_chat_result(
                        self.state,
                        rows,
                        billing_status_answer(rows),
                        "Answered from deterministic billing-status tracing.",
                        "-- billing-status trace; no LLM SQL executed",
                        resolved_focus_node_id,
                    ),
                    resolved_focus_node_id=resolved_focus_node_id,
                )
            elif is_customer_billing_question(question):
                rows = customer_billing_rows(self.state, focus_node, question)
                result = self._chat_response(
                    **build_deterministic_chat_result(
                        self.state,
                        rows,
                        customer_billing_answer(rows),
                        "Answered from deterministic customer-billing lookup.",
                        "-- customer-billing lookup; no LLM SQL executed",
                        resolved_focus_node_id,
                    ),
                    resolved_focus_node_id=resolved_focus_node_id,
                )
            elif is_customer_sales_orders_question(question):
                rows = customer_sales_order_rows(self.state, focus_node, question)
                result = self._chat_response(
                    **build_deterministic_chat_result(
                        self.state,
                        rows,
                        customer_sales_order_answer(rows),
                        "Answered from deterministic customer-sales-order lookup.",
                        "-- customer-sales-order lookup; no LLM SQL executed",
                        resolved_focus_node_id,
                    ),
                    resolved_focus_node_id=resolved_focus_node_id,
                )
            elif is_full_flow_question(question):
                rows = full_flow_rows(self.state, focus_node, question)
                result = self._chat_response(
                    **build_deterministic_chat_result(
                        self.state,
                        rows,
                        full_flow_answer(question, rows),
                        "Answered from deterministic full-flow tracing.",
                        "-- full-flow trace; no LLM SQL executed",
                        resolved_focus_node_id,
                    ),
                    resolved_focus_node_id=resolved_focus_node_id,
                )
            else:
                sql_plan, sql, rows, columns = generate_and_execute_sql(self.state, question, focus_node, history)
                result = self._chat_response(
                    answer=llm_answer(question, sql, rows, focus_node, history),
                    sql=sql,
                    rows=rows,
                    columns=columns,
                    related_node_ids=related_node_ids_from_rows(self.state, rows, resolved_focus_node_id),
                    notes=str(sql_plan.get("notes", "")),
                    resolved_focus_node_id=resolved_focus_node_id,
                )

            min_expected_related = 1 if resolved_focus_node_id else 0
            if not result["rows"] or len(result["relatedNodeIds"]) <= min_expected_related:
                fallback = graph_relation_fallback(self.state, question, resolved_focus_node_id)
                if fallback:
                    merged_notes = f"{result['notes']} {fallback['notes']}".strip()
                    result = self._chat_response(
                        answer=str(fallback["answer"]),
                        sql=str(fallback["sql"]),
                        rows=list(fallback["rows"]),
                        columns=list(fallback["columns"]),
                        related_node_ids=list(fallback["relatedNodeIds"]),
                        notes=merged_notes,
                        resolved_focus_node_id=resolved_focus_node_id,
                    )

            if is_list_intent(question):
                result["answer"] = rows_to_list_answer(result.get("rows", []))

            self._send_json(200, result)
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"error": str(exc)})


def run_server() -> None:
    load_dotenv()
    state = init_state()
    GraphChatHandler.state = state
    host = DEFAULT_HOST
    port = DEFAULT_PORT
    server = ThreadingHTTPServer((host, port), GraphChatHandler)
    print(f"DodgeChat graph app running at http://{host}:{port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        state.conn.close()


if __name__ == "__main__":
    run_server()
