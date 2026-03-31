import json
import os
import re
import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .config import (
    DEFAULT_MAX_TOKENS,
    DOMAIN_KEYWORDS,
    FOCUS_REFERENCE_PHRASES,
    GENERIC_TOKENS,
    QUESTION_ENTITY_HINTS,
)
from .runtime import (
    call_openrouter,
    extract_json_object,
    identifier_tokens_from_value,
    norm_id,
    related_tables_for_column,
    safe_sql,
)
from .state import AppState


NON_DATASET_PATTERNS = (
    r"\bweather\b",
    r"\btemperature\b",
    r"\bforecast\b",
    r"\bclimate\b",
    r"\bcapital of\b",
    r"\bpopulation of\b",
    r"\bwhich state (am i|i am|i'm) in\b",
    r"\bwhere am i\b",
    r"\bpoem\b",
    r"\bstory\b",
    r"\bfiction\b",
    r"\bjoke\b",
    r"\blyrics\b",
    r"\bcreative writing\b",
    r"\bwrite .* (poem|story|joke|song)\b",
)

LIST_INTENT_PATTERNS = (
    r"\blist\b",
    r"\bshow\b.*\blist\b",
    r"\bgive me\b.*\blist\b",
    r"\bwhich\b.*\b(all|available)\b",
    r"\bwhat are\b.*\b(all|the)\b",
)

FOLLOW_UP_PATTERNS = (
    r"\bok\b",
    r"\bonly\b",
    r"\btop\s+\d+\b",
    r"\bfirst\s+\d+\b",
    r"\blimit\s+\d+\b",
    r"\bjust\s+\d+\b",
    r"\bshow\s+(me\s+)?(only\s+)?\d+\b",
    r"\bmake it\b",
    r"\bcan you\b",
)


def is_non_dataset_intent(question: str) -> bool:
    lowered = question.lower().strip()
    if not lowered:
        return False
    return any(re.search(pattern, lowered) for pattern in NON_DATASET_PATTERNS)


def is_list_intent(question: str) -> bool:
    lowered = question.lower().strip()
    if not lowered:
        return False
    return any(re.search(pattern, lowered) for pattern in LIST_INTENT_PATTERNS)


def is_follow_up_question(question: str) -> bool:
    lowered = question.lower().strip()
    if not lowered:
        return False
    if len(lowered.split()) <= 6:
        return True
    return any(re.search(pattern, lowered) for pattern in FOLLOW_UP_PATTERNS)


def _row_line_text(row: Dict[str, Any], columns: Sequence[str]) -> str:
    preferred_keys = [
        "customer",
        "businessPartnerName",
        "businessPartnerFullName",
        "soldToParty",
        "salesOrder",
        "salesOrderItem",
        "deliveryDocument",
        "billingDocument",
        "material",
        "product",
        "productDescription",
        "plant",
        "companyCode",
        "netAmount",
        "totalNetAmount",
        "transactionCurrency",
        "status",
    ]
    parts: List[str] = []
    used = set()

    for key in preferred_keys:
        if key in row and row.get(key) not in (None, "", "null"):
            value = str(row.get(key)).strip()
            if value:
                parts.append(value if key in {"customer", "businessPartnerName", "businessPartnerFullName", "productDescription"} else f"{key}: {value}")
                used.add(key)
        if len(parts) >= 4:
            break

    if not parts:
        for key in columns:
            if key in used:
                continue
            value = row.get(key)
            if value in (None, "", "null"):
                continue
            text = str(value).strip()
            if text:
                parts.append(f"{key}: {text}")
            if len(parts) >= 4:
                break

    return ", ".join(parts)


def rows_to_list_answer(rows: Sequence[Dict[str, Any]], max_items: int = 25) -> str:
    if not rows:
        return "No matching records found in the dataset."

    columns: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    lines: List[str] = []
    seen = set()
    for row in rows:
        line = _row_line_text(row, columns)
        if not line:
            continue
        if line in seen:
            continue
        seen.add(line)
        lines.append(line)
        if len(lines) >= max_items:
            break

    if not lines:
        return "No matching records found in the dataset."

    return "\n".join(f"{index}. {line}" for index, line in enumerate(lines, start=1))


def llm_generate_sql(question: str, schema_text: str, focus_node: Optional[Dict[str, Any]], history: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    history_lines = [f'{item.get("role", "user")}: {item.get("content", "")}' for item in history[-6:]]
    focus_text = json.dumps(focus_node, ensure_ascii=True) if focus_node else "null"
    messages = [
        {
            "role": "system",
            "content": (
                "You are a data analyst for an SAP order-to-cash dataset. "
                "Translate user questions into one SQLite SELECT query. "
                "Respond with strict JSON only: {\"sql\":\"...\",\"notes\":\"...\"}. "
                "Do not use markdown. Do not invent tables or columns. "
                "Prefer canonical process links first and use weaker inferred overlap links only when no canonical join answers the question."
            ),
        },
        {
            "role": "user",
            "content": (
                f"{schema_text}\n\nSelected graph node context: {focus_text}\n"
                f"Recent chat history:\n" + "\n".join(history_lines or ["(none)"]) + "\n\n"
                f"Question: {question}\n\nReturn one read-only SQLite query that best answers the question."
            ),
        },
    ]
    return extract_json_object(call_openrouter(messages, max_tokens=500))


def llm_repair_sql(
    question: str,
    schema_text: str,
    focus_node: Optional[Dict[str, Any]],
    history: Sequence[Dict[str, str]],
    previous_sql: str,
    error_text: str,
) -> Dict[str, Any]:
    history_lines = [f'{item.get("role", "user")}: {item.get("content", "")}' for item in history[-6:]]
    focus_text = json.dumps(focus_node, ensure_ascii=True) if focus_node else "null"
    messages = [
        {
            "role": "system",
            "content": (
                "You repair SQLite queries for an SAP order-to-cash dataset. "
                "Respond with strict JSON only: {\"sql\":\"...\",\"notes\":\"...\"}. "
                "Use only real tables and columns from the schema and inferred links."
            ),
        },
        {
            "role": "user",
            "content": (
                f"{schema_text}\n\nSelected graph node context: {focus_text}\n"
                f"Recent chat history:\n" + "\n".join(history_lines or ["(none)"]) + "\n\n"
                f"Question: {question}\n"
                f"Failed SQL:\n{previous_sql}\n\n"
                f"SQLite error:\n{error_text}\n\n"
                "Return a corrected read-only SQLite query."
            ),
        },
    ]
    return extract_json_object(call_openrouter(messages, max_tokens=500))


def execute_sql(conn: sqlite3.Connection, sql: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    cursor = conn.execute(sql)
    rows = [dict(row) for row in cursor.fetchall()]
    columns = [column[0] for column in cursor.description] if cursor.description else []
    return rows, columns


def resolve_focus_node(state: AppState, question: str, focus_node_id: Optional[str]) -> Optional[str]:
    if focus_node_id and focus_node_id in state.node_details:
        return focus_node_id

    identifier_tokens = re.findall(r"\b[A-Z0-9][A-Z0-9_-]{4,}\b|\b\d{5,}\b", question, re.IGNORECASE)
    scored: Dict[str, int] = defaultdict(int)
    lowered = question.lower()
    preferred_entity = None
    if "delivery" in lowered:
        preferred_entity = "Delivery"
    elif "sales order" in lowered or "order" in lowered:
        preferred_entity = "Sales Order"
    elif "billing" in lowered or "invoice" in lowered:
        preferred_entity = "Billing Document"
    elif "journal" in lowered:
        preferred_entity = "Journal Entry"
    elif "payment" in lowered:
        preferred_entity = "Payment"
    elif "group" in lowered or "grp" in lowered:
        preferred_entity = "Product Group"

    for token in identifier_tokens:
        for node_id in state.value_index.get(norm_id(token), []):
            scored[node_id] += 5

    for node_id, details in state.node_details.items():
        label = str(details.get("label", "")).lower()
        if any(norm_id(token) == norm_id(label) for token in identifier_tokens):
            scored[node_id] += 8
        if preferred_entity and details.get("entity") == preferred_entity:
            scored[node_id] += 3

    if not scored:
        return None
    return max(scored.items(), key=lambda item: item[1])[0]


def build_focus_context(state: AppState, focus_node_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not focus_node_id:
        return None
    focus_node = state.node_details.get(focus_node_id)
    if not focus_node:
        return None
    neighbors = []
    for neighbor_id in state.adjacency.get(focus_node_id, [])[:12]:
        neighbor = state.node_details.get(neighbor_id)
        if neighbor:
            neighbors.append(
                {
                    "id": neighbor["id"],
                    "entity": neighbor["entity"],
                    "label": neighbor["label"],
                }
            )
    enriched = dict(focus_node)
    enriched["neighbors"] = neighbors
    return enriched


def is_graph_neighborhood_question(question: str, focus_node: Optional[Dict[str, Any]]) -> bool:
    if not focus_node:
        return False
    lowered = question.lower()
    triggers = [
        "about this",
        "connected records",
        "connected record",
        "connections",
        "role and connected",
        "explain the role",
        "about this delivery",
        "about this order",
        "about this sales order",
    ]
    return any(trigger in lowered for trigger in triggers)


def is_full_flow_question(question: str) -> bool:
    lowered = question.lower()
    triggers = [
        "trace the full flow",
        "full flow",
        "sales order",
        "delivery",
        "billing",
        "journal",
    ]
    return "flow" in lowered and sum(trigger in lowered for trigger in triggers) >= 2


def is_highest_billing_document_question(question: str) -> bool:
    lowered = question.lower()
    return ("highest billing document" in lowered or "largest billing document" in lowered or "top billing document" in lowered)


def is_billing_status_question(question: str) -> bool:
    lowered = question.lower()
    return "billing status" in lowered and ("sales order" in lowered or "order" in lowered)


def is_customer_sales_orders_question(question: str) -> bool:
    lowered = question.lower()
    return "customer" in lowered and ("sales order" in lowered or "sales orders" in lowered or "orders" in lowered)


def is_customer_billing_question(question: str) -> bool:
    lowered = question.lower()
    return ("customer" in lowered or "sold-to" in lowered) and ("billing" in lowered or "invoice" in lowered)



def is_dataset_domain_question(
    state: AppState,
    question: str,
    focus_node: Optional[Dict[str, Any]],
    history: Optional[Sequence[Dict[str, str]]] = None,
) -> bool:
    lowered = question.lower().strip()
    if not lowered:
        return False

    if is_non_dataset_intent(lowered):
        return False

    # Strict guardrail: free-form words are not enough.
    # We only accept explicit domain language, real dataset identifiers,
    # or an explicit reference to an already selected node/record.
    if any(keyword in lowered for keyword in DOMAIN_KEYWORDS):
        return True

    question_tokens = identifier_tokens_from_value(question)
    if any(token in state.value_index and any(char.isdigit() for char in token) for token in question_tokens):
        return True

    if focus_node and any(phrase in lowered for phrase in FOCUS_REFERENCE_PHRASES):
        return True

    if history and is_follow_up_question(question):
        recent_user_messages = [
            str(item.get("content", "")).strip()
            for item in history[-6:]
            if str(item.get("role", "")).strip().lower() == "user" and str(item.get("content", "")).strip()
        ]
        prior_messages = recent_user_messages[:-1] if recent_user_messages else []
        for prior_question in reversed(prior_messages):
            if is_dataset_domain_question(state, prior_question, focus_node=None, history=None):
                return True
    return False


def resolve_entity_values(state: AppState, question: str, focus_node: Optional[Dict[str, Any]], entity: str, property_name: Optional[str] = None) -> List[str]:
    values: List[str] = []
    seen = set()

    def add_from_detail(detail: Dict[str, Any]) -> None:
        if detail.get("entity") != entity:
            return
        raw_value = detail.get("label")
        properties = detail.get("properties", {})
        if property_name and isinstance(properties, dict):
            raw_value = properties.get(property_name) or raw_value
        normalized = str(raw_value).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            values.append(normalized)

    if focus_node:
        add_from_detail(focus_node)

    for token in identifier_tokens_from_value(question):
        for node_id in state.value_index.get(token, []):
            detail = state.node_details.get(node_id)
            if detail:
                add_from_detail(detail)

    return values


def billing_status_rows(state: AppState, focus_node: Optional[Dict[str, Any]], question: str) -> List[Dict[str, Any]]:
    sales_orders = resolve_entity_values(state, question, focus_node, "Sales Order")
    if not sales_orders:
        return []
    sales_order = sales_orders[0]
    rows = [
        dict(row)
        for row in state.conn.execute(
            """
            SELECT
                soh.salesOrder,
                soh.overallOrdReltdBillgStatus,
                soh.totalNetAmount AS salesOrderTotalNetAmount,
                soh.transactionCurrency,
                odi.deliveryDocument,
                bdi.billingDocument,
                bdh.billingDocumentIsCancelled,
                bdh.billingDocumentType,
                bdh.totalNetAmount AS billingDocumentTotalNetAmount
            FROM sales_order_headers soh
            LEFT JOIN outbound_delivery_items odi
                ON soh.salesOrder = odi.referenceSdDocument
            LEFT JOIN billing_document_items bdi
                ON odi.deliveryDocument = bdi.referenceSdDocument
               AND norm_id(odi.deliveryDocumentItem) = norm_id(bdi.referenceSdDocumentItem)
            LEFT JOIN billing_document_headers bdh
                ON bdi.billingDocument = bdh.billingDocument
            WHERE soh.salesOrder = ?
            ORDER BY odi.deliveryDocument, bdi.billingDocument
            LIMIT 80
            """,
            (sales_order,),
        ).fetchall()
    ]
    return rows


def billing_status_answer(rows: Sequence[Dict[str, Any]]) -> str:
    if not rows:
        return "I could not find a matching sales order for that billing-status question."

    sales_order = rows[0].get("salesOrder")
    header_status = str(rows[0].get("overallOrdReltdBillgStatus") or "").strip()
    status_text = {"A": "not yet billed", "B": "partially billed", "C": "completely billed"}.get(header_status, "")
    deliveries: List[str] = []
    active_billings: List[str] = []
    cancelled_billings: List[str] = []

    for row in rows:
        delivery = str(row.get("deliveryDocument") or "").strip()
        billing = str(row.get("billingDocument") or "").strip()
        cancelled = str(row.get("billingDocumentIsCancelled") or "").strip().lower() == "true"
        if delivery and delivery not in deliveries:
            deliveries.append(delivery)
        if billing:
            target = cancelled_billings if cancelled else active_billings
            if billing not in target:
                target.append(billing)

    if active_billings and cancelled_billings:
        return (
            f"Sales order {sales_order} is billed through {', '.join(active_billings[:6])}. "
            f"Earlier billing document {', '.join(cancelled_billings[:6])} was cancelled. "
            f"The header billing-status field is empty, but the downstream billing flow exists."
        )
    if active_billings:
        if status_text:
            return f"Sales order {sales_order} is {status_text} and is linked to billing document {', '.join(active_billings[:6])}."
        return f"Sales order {sales_order} has billing document {', '.join(active_billings[:6])}. The header billing-status field is empty, but downstream billing exists."
    if cancelled_billings:
        return f"Sales order {sales_order} only shows cancelled billing document {', '.join(cancelled_billings[:6])}. The header billing-status field is empty."
    if deliveries:
        return f"Sales order {sales_order} has delivery {', '.join(deliveries[:6])}, but no billing document was found. The header billing-status field is empty."
    if status_text:
        return f"Sales order {sales_order} is marked as {status_text}."
    return f"The billing-status field for sales order {sales_order} is empty, and no downstream billing document was found."


def customer_sales_order_rows(state: AppState, focus_node: Optional[Dict[str, Any]], question: str) -> List[Dict[str, Any]]:
    customers = resolve_entity_values(state, question, focus_node, "Customer", "customer")
    params: List[Any] = []
    where_clause = ""
    if customers:
        placeholders = ", ".join("?" for _ in customers)
        where_clause = f"WHERE bp.customer IN ({placeholders})"
        params.extend(customers)
    rows = [
        dict(row)
        for row in state.conn.execute(
            f"""
            SELECT
                bp.customer,
                bp.businessPartnerName,
                soh.salesOrder,
                soh.totalNetAmount,
                soh.transactionCurrency
            FROM business_partners bp
            JOIN sales_order_headers soh
                ON bp.customer = soh.soldToParty
            {where_clause}
            ORDER BY bp.customer, soh.salesOrder
            LIMIT 160
            """,
            tuple(params),
        ).fetchall()
    ]
    return rows


def customer_sales_order_answer(rows: Sequence[Dict[str, Any]]) -> str:
    if not rows:
        return "I could not find matching customer sales-order records for that question."

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        customer = str(row.get("customer") or "").strip()
        name = str(row.get("businessPartnerName") or customer).strip()
        entry = grouped.setdefault(customer, {"name": name, "orders": []})
        order = str(row.get("salesOrder") or "").strip()
        amount = str(row.get("totalNetAmount") or "").strip()
        currency = str(row.get("transactionCurrency") or "").strip()
        if order:
            entry["orders"].append((order, amount, currency))

    parts: List[str] = []
    for customer, entry in list(grouped.items())[:6]:
        orders = entry["orders"]
        if not orders:
            continue
        preview = ", ".join(
            f"{order}{f' ({amount} {currency})' if amount and currency else ''}"
            for order, amount, currency in orders[:4]
        )
        if len(orders) > 4:
            preview += f", and {len(orders) - 4} more"
        parts.append(f"{entry['name']} ({customer}) has sales orders {preview}.")
    return " ".join(parts)


def customer_billing_rows(state: AppState, focus_node: Optional[Dict[str, Any]], question: str) -> List[Dict[str, Any]]:
    customers = resolve_entity_values(state, question, focus_node, "Customer", "customer")
    params: List[Any] = []
    where_clause = ""
    if customers:
        placeholders = ", ".join("?" for _ in customers)
        where_clause = f"WHERE bp.customer IN ({placeholders})"
        params.extend(customers)
    rows = [
        dict(row)
        for row in state.conn.execute(
            f"""
            SELECT
                bp.customer,
                bp.businessPartnerName,
                bdh.billingDocument,
                bdh.billingDocumentIsCancelled,
                bdh.totalNetAmount,
                bdh.transactionCurrency
            FROM business_partners bp
            JOIN billing_document_headers bdh
                ON bp.customer = bdh.soldToParty
            {where_clause}
            ORDER BY bp.customer, bdh.billingDocument
            LIMIT 160
            """,
            tuple(params),
        ).fetchall()
    ]
    return rows


def customer_billing_answer(rows: Sequence[Dict[str, Any]]) -> str:
    if not rows:
        return "I could not find matching customer billing records for that question."

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        customer = str(row.get("customer") or "").strip()
        name = str(row.get("businessPartnerName") or customer).strip()
        entry = grouped.setdefault(customer, {"name": name, "billing": []})
        billing_document = str(row.get("billingDocument") or "").strip()
        amount = str(row.get("totalNetAmount") or "").strip()
        currency = str(row.get("transactionCurrency") or "").strip()
        cancelled = str(row.get("billingDocumentIsCancelled") or "").strip().lower() == "true"
        if billing_document:
            entry["billing"].append((billing_document, amount, currency, cancelled))

    parts: List[str] = []
    for customer, entry in list(grouped.items())[:6]:
        billing_rows = entry["billing"]
        if not billing_rows:
            continue
        preview = ", ".join(
            f"{doc}{' cancelled' if cancelled else ''}{f' ({amount} {currency})' if amount and currency else ''}"
            for doc, amount, currency, cancelled in billing_rows[:4]
        )
        if len(billing_rows) > 4:
            preview += f", and {len(billing_rows) - 4} more"
        parts.append(f"{entry['name']} ({customer}) has billing documents {preview}.")
    return " ".join(parts)


def full_flow_rows(state: AppState, focus_node: Optional[Dict[str, Any]], question: str) -> List[Dict[str, Any]]:
    identifier_tokens = identifier_tokens_from_value(question)
    sales_order = None
    billing_document = None
    if focus_node and focus_node.get("entity") == "Sales Order":
        sales_order = focus_node.get("label")
    if focus_node and focus_node.get("entity") == "Billing Document":
        billing_document = focus_node.get("label")
    if not sales_order:
        for token in identifier_tokens:
            for node_id in state.value_index.get(token, []):
                detail = state.node_details.get(node_id)
                if detail and detail.get("entity") == "Sales Order":
                    sales_order = detail.get("label")
                    break
            if sales_order:
                break
    if not billing_document:
        for token in identifier_tokens:
            for node_id in state.value_index.get(token, []):
                detail = state.node_details.get(node_id)
                if detail and detail.get("entity") == "Billing Document":
                    billing_document = detail.get("label")
                    break
            if billing_document:
                break

    if not billing_document and is_highest_billing_document_question(question):
        row = state.conn.execute(
            """
            SELECT billingDocument
            FROM billing_document_headers
            WHERE totalNetAmount IS NOT NULL AND TRIM(totalNetAmount) != ''
            ORDER BY CAST(totalNetAmount AS REAL) DESC, billingDocument DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            billing_document = row["billingDocument"]

    if sales_order:
        rows = [
            dict(row)
            for row in state.conn.execute(
                """
                SELECT
                    soh.salesOrder,
                    soh.soldToParty,
                    soh.totalNetAmount AS salesOrderTotalNetAmount,
                    soh.transactionCurrency,
                    odi.deliveryDocument,
                    odh.shippingPoint,
                    bdi.billingDocument,
                    bdh.accountingDocument,
                    bdh.billingDocumentType,
                    bdh.billingDocumentIsCancelled
                FROM sales_order_headers soh
                LEFT JOIN outbound_delivery_items odi
                    ON soh.salesOrder = odi.referenceSdDocument
                LEFT JOIN outbound_delivery_headers odh
                    ON odi.deliveryDocument = odh.deliveryDocument
                LEFT JOIN billing_document_items bdi
                    ON odi.deliveryDocument = bdi.referenceSdDocument
                   AND norm_id(odi.deliveryDocumentItem) = norm_id(bdi.referenceSdDocumentItem)
                LEFT JOIN billing_document_headers bdh
                    ON bdi.billingDocument = bdh.billingDocument
                WHERE soh.salesOrder = ?
                ORDER BY odi.deliveryDocument, bdi.billingDocument
                LIMIT 50
                """,
                (sales_order,),
            ).fetchall()
        ]
        return rows

    if not billing_document:
        return []

    rows = [
        dict(row)
        for row in state.conn.execute(
            """
            SELECT
                soh.salesOrder,
                soh.soldToParty,
                soh.totalNetAmount AS salesOrderTotalNetAmount,
                soh.transactionCurrency,
                odi.deliveryDocument,
                odh.shippingPoint,
                bdh.billingDocument,
                bdh.accountingDocument,
                bdh.billingDocumentType,
                bdh.billingDocumentIsCancelled,
                bdh.totalNetAmount AS billingDocumentTotalNetAmount
            FROM billing_document_headers bdh
            LEFT JOIN billing_document_items bdi
                ON bdh.billingDocument = bdi.billingDocument
            LEFT JOIN outbound_delivery_items odi
                ON bdi.referenceSdDocument = odi.deliveryDocument
               AND norm_id(bdi.referenceSdDocumentItem) = norm_id(odi.deliveryDocumentItem)
            LEFT JOIN outbound_delivery_headers odh
                ON odi.deliveryDocument = odh.deliveryDocument
            LEFT JOIN sales_order_headers soh
                ON odi.referenceSdDocument = soh.salesOrder
            WHERE bdh.billingDocument = ?
            ORDER BY soh.salesOrder, odi.deliveryDocument
            LIMIT 50
            """,
            (billing_document,),
        ).fetchall()
    ]
    return rows


def full_flow_answer(question: str, rows: Sequence[Dict[str, Any]]) -> str:
    if not rows:
        return "I could not find a matching order-to-cash flow for that request."

    sales_order = rows[0].get("salesOrder")
    billing_document = rows[0].get("billingDocument")
    amount = rows[0].get("salesOrderTotalNetAmount")
    currency = rows[0].get("transactionCurrency")
    deliveries = []
    billings = []
    accounting_docs = []
    for row in rows:
        delivery = row.get("deliveryDocument")
        billing = row.get("billingDocument")
        accounting = row.get("accountingDocument")
        if delivery and delivery not in deliveries:
            deliveries.append(delivery)
        if billing and billing not in billings:
            billings.append(billing)
        if accounting and accounting not in accounting_docs:
            accounting_docs.append(accounting)

    parts = []
    if sales_order:
        parts.append(f"Sales order {sales_order} has a total amount of {amount} {currency}.")
    elif billing_document:
        billing_amount = rows[0].get("billingDocumentTotalNetAmount")
        parts.append(f"Billing document {billing_document} is part of the traced order-to-cash flow with amount {billing_amount} {currency}.")
    if deliveries:
        parts.append(f"It flows to delivery {', '.join(deliveries[:6])}.")
    if billings:
        parts.append(f"It is billed through {', '.join(billings[:6])}.")
    if accounting_docs:
        parts.append(f"The linked accounting document{'s are' if len(accounting_docs) > 1 else ' is'} {', '.join(accounting_docs[:6])}.")
    if not accounting_docs:
        parts.append("No journal entry link was found in the available accounts receivable journal dataset.")
    return " ".join(parts)


def graph_context_rows(state: AppState, focus_node: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.append(
        {
            "type": "focus",
            "entity": focus_node["entity"],
            "label": focus_node["label"],
            "table": focus_node["table"],
            "connections": focus_node.get("connections", 0),
            **focus_node.get("properties", {}),
        }
    )
    for neighbor in focus_node.get("neighbors", []):
        detail = state.node_details.get(neighbor["id"])
        if not detail:
            continue
        rows.append(
            {
                "type": "neighbor",
                "entity": detail["entity"],
                "label": detail["label"],
                "table": detail["table"],
                **detail.get("properties", {}),
            }
        )
    return rows


def graph_context_answer(question: str, focus_node: Dict[str, Any], rows: Sequence[Dict[str, Any]]) -> str:
    summary_rows = json.dumps(rows[:20], ensure_ascii=True, indent=2)
    messages = [
        {
            "role": "system",
            "content": (
                "You explain a graph node and its connected records using only the provided graph context rows. "
                "Be concise, data-backed, and mention identifiers and linked entities. "
                "Do not use markdown, bold, bullet styling, or asterisks. "
                "Keep the answer short and to the point, ideally 2 to 4 sentences."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Question: {question}\n"
                f"Focus node:\n{json.dumps(focus_node, ensure_ascii=True, indent=2)}\n\n"
                f"Connected graph rows:\n{summary_rows}\n\n"
                "Answer in plain natural language only. Do not use markdown or asterisks. "
                "Keep it brief, focused, and natural. Do not add meta commentary about the source of the answer."
            ),
        },
    ]
    return call_openrouter(messages, max_tokens=int(os.getenv("OPENROUTER_MAX_TOKENS", str(DEFAULT_MAX_TOKENS))))


def generate_and_execute_sql(
    state: AppState,
    question: str,
    focus_node: Optional[Dict[str, Any]],
    history: Sequence[Dict[str, str]],
) -> Tuple[Dict[str, Any], str, List[Dict[str, Any]], List[str]]:
    sql_plan = llm_generate_sql(question, state.schema_text, focus_node, history)
    sql = safe_sql(str(sql_plan.get("sql", "")))
    try:
        rows, columns = execute_sql(state.conn, sql)
        return sql_plan, sql, rows, columns
    except sqlite3.OperationalError as exc:
        repaired = llm_repair_sql(question, state.schema_text, focus_node, history, sql, str(exc))
        repaired_sql = safe_sql(str(repaired.get("sql", "")))
        rows, columns = execute_sql(state.conn, repaired_sql)
        return repaired, repaired_sql, rows, columns


def find_related_node_ids(
    value_index: Dict[str, List[str]],
    node_details: Dict[str, Dict[str, Any]],
    rows: Sequence[Dict[str, Any]],
    focus_node_id: Optional[str],
) -> List[str]:
    node_ids: List[str] = []
    seen = set()
    if focus_node_id:
        node_ids.append(focus_node_id)
        seen.add(focus_node_id)
    for row in rows:
        for column_name, value in row.items():
            preferred_tables = related_tables_for_column(column_name)
            if not preferred_tables:
                continue
            for token in identifier_tokens_from_value(value):
                if token in GENERIC_TOKENS or token in {"true", "false", "a", "b", "c"}:
                    continue
                candidate_ids = value_index.get(token, [])
                if preferred_tables:
                    filtered_ids = [
                        node_id
                        for node_id in candidate_ids
                        if node_details.get(node_id, {}).get("table") in preferred_tables
                    ]
                    if filtered_ids:
                        candidate_ids = filtered_ids
                for node_id in candidate_ids:
                    if node_id not in seen:
                        seen.add(node_id)
                        node_ids.append(node_id)
                    if len(node_ids) >= 120:
                        return node_ids
    return node_ids


def shortest_path_between(adjacency: Dict[str, List[str]], start_id: str, goal_id: str, max_depth: int = 6) -> List[str]:
    if not start_id or not goal_id:
        return []
    if start_id == goal_id:
        return [start_id]

    queue: List[Tuple[str, List[str], int]] = [(start_id, [start_id], 0)]
    seen = {start_id}
    while queue:
        current_id, path, depth = queue.pop(0)
        if depth >= max_depth:
            continue
        for neighbor_id in adjacency.get(current_id, []):
            if neighbor_id in seen:
                continue
            next_path = path + [neighbor_id]
            if neighbor_id == goal_id:
                return next_path
            seen.add(neighbor_id)
            queue.append((neighbor_id, next_path, depth + 1))
    return []


def expand_related_node_ids_with_paths(state: AppState, node_ids: Sequence[str], focus_node_id: Optional[str]) -> List[str]:
    ordered_ids = [node_id for node_id in node_ids if node_id in state.node_details]
    if not ordered_ids:
        return []

    root_id = focus_node_id if focus_node_id in state.node_details else ordered_ids[0]
    expanded: List[str] = []
    seen = set()

    def add_node(node_id: str) -> None:
        if node_id and node_id not in seen:
            seen.add(node_id)
            expanded.append(node_id)

    add_node(root_id)
    for node_id in ordered_ids:
        add_node(node_id)

    for node_id in ordered_ids[:32]:
        if node_id == root_id:
            continue
        for path_node_id in shortest_path_between(state.adjacency, root_id, node_id):
            add_node(path_node_id)
        if len(expanded) >= 180:
            break

    if len(expanded) < 180 and len(ordered_ids) > 1:
        for index in range(min(len(ordered_ids) - 1, 16)):
            start_id = ordered_ids[index]
            end_id = ordered_ids[index + 1]
            for path_node_id in shortest_path_between(state.adjacency, start_id, end_id):
                add_node(path_node_id)
            if len(expanded) >= 180:
                break

    return expanded[:180]


def related_node_ids_from_rows(
    state: AppState,
    rows: Sequence[Dict[str, Any]],
    focus_node_id: Optional[str],
) -> List[str]:
    return expand_related_node_ids_with_paths(
        state,
        find_related_node_ids(state.value_index, state.node_details, rows, focus_node_id),
        focus_node_id,
    )


def build_deterministic_chat_result(
    state: AppState,
    rows: Sequence[Dict[str, Any]],
    answer: str,
    note: str,
    sql: str,
    focus_node_id: Optional[str],
) -> Dict[str, Any]:
    return {
        "answer": answer,
        "sql": sql,
        "rows": list(rows),
        "columns": sorted({key for row in rows for key in row.keys()}) if rows else [],
        "related_node_ids": related_node_ids_from_rows(state, rows, focus_node_id),
        "notes": note,
    }


def preferred_entities_from_question(question: str) -> List[str]:
    lowered = question.lower()
    preferred: List[str] = []
    seen = set()
    for phrase, entity in QUESTION_ENTITY_HINTS:
        if phrase in lowered and entity not in seen:
            preferred.append(entity)
            seen.add(entity)
    return preferred


def resolve_question_node_ids(state: AppState, question: str, focus_node_id: Optional[str]) -> List[str]:
    node_ids: List[str] = []
    seen = set()
    preferred_entities = preferred_entities_from_question(question)

    def add_node(node_id: str) -> None:
        if node_id in state.node_details and node_id not in seen:
            seen.add(node_id)
            node_ids.append(node_id)

    if focus_node_id:
        add_node(focus_node_id)

    for token in identifier_tokens_from_value(question):
        candidate_ids = list(state.value_index.get(token, []))
        if preferred_entities:
            filtered = [
                node_id
                for node_id in candidate_ids
                if state.node_details.get(node_id, {}).get("entity") in preferred_entities
            ]
            if filtered:
                candidate_ids = filtered
        candidate_ids.sort(
            key=lambda node_id: (
                0 if state.node_details.get(node_id, {}).get("entity") in preferred_entities else 1,
                -int(state.node_details.get(node_id, {}).get("connections", 0)),
                str(state.node_details.get(node_id, {}).get("label", "")),
            )
        )
        for node_id in candidate_ids[:12]:
            add_node(node_id)

    if len(node_ids) <= 1 and focus_node_id and preferred_entities:
        queue: List[Tuple[str, int]] = [(focus_node_id, 0)]
        visited = {focus_node_id}
        while queue and len(node_ids) < 18:
            current_id, depth = queue.pop(0)
            if depth >= 8:
                continue
            for neighbor_id in state.adjacency.get(current_id, []):
                if neighbor_id in visited:
                    continue
                visited.add(neighbor_id)
                neighbor_entity = state.node_details.get(neighbor_id, {}).get("entity")
                if neighbor_entity in preferred_entities:
                    add_node(neighbor_id)
                queue.append((neighbor_id, depth + 1))

    return node_ids[:24]


def build_graph_relation_rows(state: AppState, path_node_ids: Sequence[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for index, node_id in enumerate(path_node_ids, start=1):
        detail = state.node_details.get(node_id)
        if not detail:
            continue
        row = {
            "step": index,
            "entity": detail.get("entity"),
            "label": detail.get("label"),
            "table": detail.get("table"),
        }
        row.update(detail.get("properties", {}))
        rows.append(row)
    return rows


def graph_relation_answer(question: str, path_node_ids: Sequence[str], state: AppState) -> str:
    details = [state.node_details.get(node_id) for node_id in path_node_ids if node_id in state.node_details]
    details = [detail for detail in details if detail]
    if len(details) < 2:
        return "I could not find a connected graph path for that question."

    lowered = question.lower()

    def first_detail(entity_name: str) -> Optional[Dict[str, Any]]:
        for item in details:
            if str(item.get("entity", "")).lower() == entity_name.lower():
                return item
        return None

    def first_prop(detail: Optional[Dict[str, Any]], keys: Sequence[str]) -> str:
        if not detail:
            return ""
        props = detail.get("properties", {}) or {}
        for key in keys:
            value = props.get(key)
            if value not in (None, "", "null"):
                return str(value).strip()
        return ""

    if "product" in lowered and ("name" in lowered or "what is" in lowered):
        product_detail = first_detail("Product") or details[-1]
        product_name = first_prop(product_detail, ("productDescription", "description", "materialName"))
        product_label = str(product_detail.get("label", "")).strip() if product_detail else ""
        if product_name:
            return f"Product name: {product_name}."
        if product_label:
            return f"Product: {product_label}."

    if "delivery" in lowered and "status" in lowered:
        delivery_detail = first_detail("Delivery")
        delivery_label = str(delivery_detail.get("label", "")).strip() if delivery_detail else ""
        goods_status = first_prop(delivery_detail, ("overallGoodsMovementStatus",))
        picking_status = first_prop(delivery_detail, ("overallPickingStatus",))
        status_parts = []
        if goods_status:
            status_parts.append(f"goods movement status {goods_status}")
        if picking_status:
            status_parts.append(f"picking status {picking_status}")
        if delivery_label and status_parts:
            return f"Delivery {delivery_label} has {', '.join(status_parts)}."
        if delivery_label:
            return f"Delivery: {delivery_label}."

    chain = " -> ".join(f"{detail['entity']} {detail['label']}" for detail in details[:7])
    if len(details) > 7:
        chain += " -> ..."
    return f"Connected path: {chain}."


def graph_relation_fallback(
    state: AppState,
    question: str,
    focus_node_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    candidate_ids = resolve_question_node_ids(state, question, focus_node_id)
    if len(candidate_ids) < 2:
        return None

    root_id = focus_node_id if focus_node_id in state.node_details else candidate_ids[0]
    best_path: List[str] = []

    for candidate_id in candidate_ids:
        if candidate_id == root_id:
            continue
        path = shortest_path_between(state.adjacency, root_id, candidate_id, max_depth=10)
        if len(path) >= 2 and (not best_path or len(path) < len(best_path)):
            best_path = path

    if not best_path:
        for start_index in range(min(len(candidate_ids) - 1, 8)):
            for end_index in range(start_index + 1, min(len(candidate_ids), 10)):
                path = shortest_path_between(state.adjacency, candidate_ids[start_index], candidate_ids[end_index], max_depth=10)
                if len(path) >= 2 and (not best_path or len(path) < len(best_path)):
                    best_path = path

    if not best_path:
        return None

    expanded_node_ids = expand_related_node_ids_with_paths(state, best_path, root_id)
    rows = build_graph_relation_rows(state, best_path)
    return {
        "answer": graph_relation_answer(question, best_path, state),
        "rows": rows,
        "columns": ["step", "entity", "label", "table"],
        "relatedNodeIds": expanded_node_ids,
        "sql": "-- graph-relation fallback; no SQL executed",
        "notes": "Answered from graph relation fallback using indirect dataset paths.",
    }


def llm_answer(question: str, sql: str, rows: Sequence[Dict[str, Any]], focus_node: Optional[Dict[str, Any]], history: Sequence[Dict[str, str]]) -> str:
    history_lines = [f'{item.get("role", "user")}: {item.get("content", "")}' for item in history[-6:]]
    result_json = json.dumps(rows[:20], ensure_ascii=True, indent=2)
    focus_text = json.dumps(focus_node, ensure_ascii=True) if focus_node else "null"
    messages = [
        {
            "role": "system",
            "content": (
                "You answer questions about an SAP order-to-cash graph using only the SQL result provided. "
                "Be concise, data-backed, and mention important identifiers. "
                "If no rows were returned, say the dataset query found no matching records. "
                "Do not use markdown, bold, bullet styling, or asterisks. "
                "Keep the answer short and to the point, ideally 2 to 4 sentences."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Selected graph node context: {focus_text}\n"
                f"Recent chat history:\n" + "\n".join(history_lines or ["(none)"]) + "\n\n"
                f"Question: {question}\nSQL used:\n{sql}\n\nSQL rows:\n{result_json}\n\n"
                "Answer the question in plain natural language only. Do not use markdown or asterisks. "
                "Keep it brief, focused, and natural. Do not add meta commentary like saying the answer is based on aggregated results."
            ),
        },
    ]
    return call_openrouter(messages, max_tokens=int(os.getenv("OPENROUTER_MAX_TOKENS", str(DEFAULT_MAX_TOKENS))))
