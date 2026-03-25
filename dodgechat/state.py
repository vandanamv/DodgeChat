import json
import math
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .config import (
    CACHE_VERSION,
    FIXED_PROCESS_LINKS,
    GENERIC_TOKENS,
    GRAPH_CONFIG,
    GRAPH_EDGE_LIMIT_PER_LINK,
    HUB_ENTITY_GROUPS,
    HUB_ENTITY_PRIORITY,
    INFERENCE_MAX_VALUES,
    INFERENCE_MIN_OVERLAP,
    INFERENCE_MIN_RATIO,
    LOW_SIGNAL_JOIN_FIELDS,
    SYNTHETIC_NODE_CONFIG,
    UI_DEFAULT_ENTITY_LIMIT,
    UI_ENTITY_LIMITS,
    UI_EXCLUDED_ENTITIES,
    UI_HUB_LAYOUTS,
    UI_SYNTHETIC_LINKS,
)
from .paths import CACHE_DIR, DATASET_DIR, GRAPH_CACHE_PATH, SQLITE_CACHE_PATH
from .runtime import (
    build_label,
    compute_dataset_signature,
    json_value,
    link_signature,
    load_cached_graph_state,
    node_key,
    norm_id,
    save_cached_graph_state,
)

@dataclass
class AppState:
    conn: sqlite3.Connection
    schema_text: str
    graph_payload: Dict[str, Any]
    ui_graph_payload: Dict[str, Any]
    value_index: Dict[str, List[str]]
    node_details: Dict[str, Dict[str, Any]]
    inferred_links: List[Dict[str, Any]]
    adjacency: Dict[str, List[str]]


def build_sqlite_database(db_path: Path, force_rebuild: bool = False) -> sqlite3.Connection:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if force_rebuild and db_path.exists():
        db_path.unlink()

    if db_path.exists():
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.create_function("norm_id", 1, norm_id)
        return conn

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.create_function("norm_id", 1, norm_id)

    table_columns: Dict[str, List[str]] = {}
    for table_dir in sorted(path for path in DATASET_DIR.iterdir() if path.is_dir()):
        columns = set()
        for jsonl_path in sorted(table_dir.glob("*.jsonl")):
            with jsonl_path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    columns.update(json.loads(raw_line).keys())
        ordered_columns = sorted(columns)
        table_columns[table_dir.name] = ordered_columns
        column_sql = ", ".join(f'"{column}" TEXT' for column in ordered_columns)
        conn.execute(f'CREATE TABLE "{table_dir.name}" ("__row_id" INTEGER PRIMARY KEY AUTOINCREMENT, {column_sql})')

    for table_dir in sorted(path for path in DATASET_DIR.iterdir() if path.is_dir()):
        columns = table_columns[table_dir.name]
        placeholders = ", ".join("?" for _ in columns)
        column_names = ", ".join(f'"{column}"' for column in columns)
        insert_sql = f'INSERT INTO "{table_dir.name}" ({column_names}) VALUES ({placeholders})'
        for jsonl_path in sorted(table_dir.glob("*.jsonl")):
            with jsonl_path.open("r", encoding="utf-8") as handle:
                batch = []
                for raw_line in handle:
                    row = json.loads(raw_line)
                    batch.append([json_value(row.get(column)) for column in columns])
                    if len(batch) >= 500:
                        conn.executemany(insert_sql, batch)
                        batch.clear()
                if batch:
                    conn.executemany(insert_sql, batch)
    conn.commit()
    return conn


def fetch_table_schema(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    schema: Dict[str, List[str]] = {}
    tables = [
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    ]
    for table in tables:
        columns = [row["name"] for row in conn.execute(f'PRAGMA table_info("{table}")') if row["name"] != "__row_id"]
        schema[table] = columns
    return schema


def field_name_score(field_name: str) -> int:
    normalized = re.sub(r"[^a-z0-9]", "", field_name.lower())
    keywords = ["salesorder", "deliverydocument", "billingdocument", "accountingdocument", "customer", "soldtoparty", "material", "product", "plant", "reference", "item", "businesspartner"]
    score = 0
    for keyword in keywords:
        if keyword in normalized:
            score += 1
    return score


def collect_field_samples(conn: sqlite3.Connection, schema: Dict[str, List[str]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    samples: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for table, columns in schema.items():
        for column in columns:
            if field_name_score(column) == 0:
                continue
            values = []
            seen = set()
            query = f'SELECT "{column}" AS value FROM "{table}" WHERE "{column}" IS NOT NULL AND TRIM("{column}") != "" LIMIT 1200'
            for row in conn.execute(query):
                token = norm_id(row["value"])
                if not token or token in GENERIC_TOKENS or len(token) < 2:
                    continue
                if token not in seen:
                    seen.add(token)
                    values.append(token)
                if len(values) >= INFERENCE_MAX_VALUES:
                    break
            if len(values) >= 4:
                samples[(table, column)] = {
                    "values": set(values),
                    "count": len(values),
                    "score": field_name_score(column),
                }
    return samples


def infer_links(conn: sqlite3.Connection, schema: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    samples = collect_field_samples(conn, schema)
    inferred = []
    fixed_signatures = {link_signature(link) for link in FIXED_PROCESS_LINKS}
    fixed_table_pairs = {
        tuple(sorted((str(link["source_table"]), str(link["target_table"]))))
        for link in FIXED_PROCESS_LINKS
    }
    items = list(samples.items())
    for index, ((source_table, source_field), source_meta) in enumerate(items):
        for (target_table, target_field), target_meta in items[index + 1:]:
            if source_table == target_table:
                continue
            if link_signature(
                {
                    "source_table": source_table,
                    "source_field": source_field,
                    "target_table": target_table,
                    "target_field": target_field,
                }
            ) in fixed_signatures:
                continue
            overlap = source_meta["values"] & target_meta["values"]
            overlap_count = len(overlap)
            if overlap_count < INFERENCE_MIN_OVERLAP:
                continue
            ratio = overlap_count / min(source_meta["count"], target_meta["count"])
            name_match = source_field.lower() == target_field.lower()
            source_norm = re.sub(r"[^a-z0-9]", "", source_field.lower())
            target_norm = re.sub(r"[^a-z0-9]", "", target_field.lower())
            low_signal = source_norm in LOW_SIGNAL_JOIN_FIELDS or target_norm in LOW_SIGNAL_JOIN_FIELDS
            canonical_pair = tuple(sorted((source_table, target_table))) in fixed_table_pairs
            if ratio < INFERENCE_MIN_RATIO and not name_match:
                continue
            if low_signal and ratio < 0.85 and not name_match:
                continue
            if canonical_pair and not name_match and ratio < 0.55:
                continue
            inferred.append(
                {
                    "source_table": source_table,
                    "source_field": source_field,
                    "target_table": target_table,
                    "target_field": target_field,
                    "overlap_count": overlap_count,
                    "overlap_ratio": round(ratio, 3),
                    "label": f"{source_field} -> {target_field}" if not name_match else source_field,
                }
            )
    inferred.sort(key=lambda item: (item["overlap_ratio"], item["overlap_count"]), reverse=True)
    selected = []
    source_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    target_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    for link in inferred:
        source_key = (link["source_table"], link["source_field"])
        target_key = (link["target_table"], link["target_field"])
        exact_name = link["source_field"].lower() == link["target_field"].lower()
        has_reference = "reference" in link["source_field"].lower() or "reference" in link["target_field"].lower()
        source_norm = re.sub(r"[^a-z0-9]", "", link["source_field"].lower())
        target_norm = re.sub(r"[^a-z0-9]", "", link["target_field"].lower())
        low_signal = source_norm in LOW_SIGNAL_JOIN_FIELDS or target_norm in LOW_SIGNAL_JOIN_FIELDS
        if source_counts[source_key] >= 2:
            continue
        if target_counts[target_key] >= 3:
            continue
        if link["overlap_ratio"] < 0.4 and not exact_name and not has_reference:
            continue
        if low_signal and not exact_name and not has_reference:
            continue
        selected.append(link)
        source_counts[source_key] += 1
        target_counts[target_key] += 1
        if len(selected) >= 36:
            break
    return selected


def schema_to_text(schema: Dict[str, List[str]], inferred_links: Sequence[Dict[str, Any]]) -> str:
    lines = ["Available SQLite tables and columns:"]
    for table, columns in schema.items():
        lines.append(f"- {table}: {', '.join(columns)}")
    lines.append("")
    fixed_signatures = {link_signature(link) for link in FIXED_PROCESS_LINKS}
    canonical_links = [link for link in inferred_links if link_signature(link) in fixed_signatures]
    inferred_only_links = [link for link in inferred_links if link_signature(link) not in fixed_signatures]

    lines.append("Canonical process links to prefer:")
    for link in canonical_links[:40]:
        lines.append(
            f"- {link['source_table']}.{link['source_field']} <-> {link['target_table']}.{link['target_field']} "
            f"(overlap={link['overlap_count']}, ratio={link['overlap_ratio']})"
        )
    lines.append("")
    lines.append("Additional inferred field links from the dataset:")
    for link in inferred_only_links[:20]:
        lines.append(
            f"- {link['source_table']}.{link['source_field']} <-> {link['target_table']}.{link['target_field']} "
            f"(overlap={link['overlap_count']}, ratio={link['overlap_ratio']})"
        )
    lines.append("")
    lines.append("Use SQLite syntax. Every query must be read-only. Prefer SELECT statements with LIMIT 50 or fewer.")
    lines.append("Use norm_id(value) when item numbers may have leading zeros like 10 and 000010.")
    lines.append(
        "Prefer canonical process joins first. For order-to-cash flow, follow Customer -> Sales Order -> Delivery Item/Delivery -> "
        "Billing Item/Billing Document -> Journal Entry/Payment. For master data, follow Product -> Product Description/Product Plant/"
        "Product Storage -> Plant and Customer -> Address/Sales Area/Company Assignment."
    )
    return "\n".join(lines)


def build_graph(
    conn: sqlite3.Connection,
    inferred_links: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, List[str]], Dict[str, Dict[str, Any]]]:
    node_maps: Dict[str, Dict[str, str]] = defaultdict(dict)
    row_to_node_id: Dict[Tuple[str, int], str] = {}
    nodes: List[Dict[str, Any]] = []
    node_details: Dict[str, Dict[str, Any]] = {}
    value_index: Dict[str, set] = defaultdict(set)
    edge_seen = set()
    edges: List[Dict[str, Any]] = []

    for table, config in GRAPH_CONFIG.items():
        for sql_row in conn.execute(f'SELECT * FROM "{table}"').fetchall():
            row = dict(sql_row)
            key = node_key(row, config["id_fields"])
            if not key:
                continue
            node_id = f"{table}:{key}"
            node_maps[table][key] = node_id
            row_to_node_id[(table, row["__row_id"])] = node_id
            label = build_label(row, config["label_fields"], config["entity"])
            detail_pairs = []
            for field in config["detail_fields"]:
                value = row.get(field)
                if value not in (None, ""):
                    detail_pairs.append({"field": field, "value": value})
            properties = {column: value for column, value in row.items() if column != "__row_id" and value not in (None, "")}
            nodes.append(
                {
                    "id": node_id,
                    "label": label,
                    "entity": config["entity"],
                    "table": table,
                    "color": config["color"],
                    "size": config["size"],
                    "details": detail_pairs,
                    "connections": 0,
                }
            )
            node_details[node_id] = {
                "id": node_id,
                "label": label,
                "entity": config["entity"],
                "table": table,
                "details": detail_pairs,
                "properties": properties,
                "connections": 0,
            }
            for value in properties.values():
                token = norm_id(value)
                if token:
                    value_index[token].add(node_id)

    synthetic_node_maps: Dict[str, Dict[str, str]] = defaultdict(dict)

    product_groups: Dict[str, Dict[str, Any]] = {}
    for product_row in conn.execute(
        'SELECT "productGroup", COUNT(*) AS productCount FROM "products" '
        'WHERE "productGroup" IS NOT NULL AND TRIM("productGroup") != "" '
        'GROUP BY "productGroup"'
    ):
        raw_group = str(product_row["productGroup"]).strip()
        token = norm_id(raw_group)
        if not token:
            continue
        meta = product_groups.setdefault(
            token,
            {
                "group": raw_group,
                "productCount": 0,
                "salesOrderItemCount": 0,
            },
        )
        meta["productCount"] = int(product_row["productCount"] or 0)

    for item_row in conn.execute(
        'SELECT "materialGroup", COUNT(*) AS itemCount FROM "sales_order_items" '
        'WHERE "materialGroup" IS NOT NULL AND TRIM("materialGroup") != "" '
        'GROUP BY "materialGroup"'
    ):
        raw_group = str(item_row["materialGroup"]).strip()
        token = norm_id(raw_group)
        if not token:
            continue
        meta = product_groups.setdefault(
            token,
            {
                "group": raw_group,
                "productCount": 0,
                "salesOrderItemCount": 0,
            },
        )
        meta["salesOrderItemCount"] = int(item_row["itemCount"] or 0)

    for token, meta in product_groups.items():
        node_id = f"product_groups:{token}"
        synthetic_node_maps["product_groups"][token] = node_id
        detail_pairs = [
            {"field": "productGroup", "value": meta["group"]},
            {"field": "products", "value": str(meta["productCount"])},
            {"field": "salesOrderItems", "value": str(meta["salesOrderItemCount"])},
        ]
        properties = {
            "productGroup": meta["group"],
            "products": str(meta["productCount"]),
            "salesOrderItems": str(meta["salesOrderItemCount"]),
        }
        nodes.append(
            {
                "id": node_id,
                "label": meta["group"],
                "entity": SYNTHETIC_NODE_CONFIG["product_groups"]["entity"],
                "table": "product_groups",
                "color": SYNTHETIC_NODE_CONFIG["product_groups"]["color"],
                "size": SYNTHETIC_NODE_CONFIG["product_groups"]["size"],
                "details": detail_pairs,
                "connections": 0,
            }
        )
        node_details[node_id] = {
            "id": node_id,
            "label": meta["group"],
            "entity": SYNTHETIC_NODE_CONFIG["product_groups"]["entity"],
            "table": "product_groups",
            "details": detail_pairs,
            "properties": properties,
            "connections": 0,
        }
        value_index[token].add(node_id)
        value_index[norm_id(meta["group"])].add(node_id)

    def connect_group_edges(
        source_table: str,
        source_field: str,
        group_field_name: str,
        edge_label: str,
    ) -> None:
        for row in conn.execute(
            f'SELECT "__row_id", "{source_field}" AS groupValue FROM "{source_table}" '
            f'WHERE "{source_field}" IS NOT NULL AND TRIM("{source_field}") != ""'
        ):
            token = norm_id(row["groupValue"])
            if not token:
                continue
            source_node_id = row_to_node_id.get((source_table, row["__row_id"]))
            group_node_id = synthetic_node_maps["product_groups"].get(token)
            if not source_node_id or not group_node_id or source_node_id == group_node_id:
                continue
            pair = tuple(sorted((source_node_id, group_node_id)) + [edge_label])
            if pair in edge_seen:
                continue
            edge_seen.add(pair)
            edges.append(
                {
                    "id": f"edge:{len(edges) + 1}",
                    "source": group_node_id,
                    "target": source_node_id,
                    "label": edge_label,
                    "strength": 1.0,
                }
            )
            node_details[group_node_id]["connections"] += 1
            node_details[source_node_id]["connections"] += 1

    def connect_compound_edges(
        source_table: str,
        source_fields: Sequence[str],
        target_table: str,
        target_fields: Sequence[str],
        edge_label: str,
    ) -> None:
        target_lookup: Dict[Tuple[str, ...], List[str]] = defaultdict(list)
        target_select = ", ".join(f'"{field}"' for field in target_fields)
        for target_row in conn.execute(
            f'SELECT "__row_id", {target_select} FROM "{target_table}"'
        ):
            key = tuple(norm_id(target_row[field]) for field in target_fields)
            if not all(key):
                continue
            target_node_id = row_to_node_id.get((target_table, target_row["__row_id"]))
            if target_node_id:
                target_lookup[key].append(target_node_id)

        source_select = ", ".join(f'"{field}"' for field in source_fields)
        for source_row in conn.execute(
            f'SELECT "__row_id", {source_select} FROM "{source_table}"'
        ):
            key = tuple(norm_id(source_row[field]) for field in source_fields)
            if not all(key):
                continue
            source_node_id = row_to_node_id.get((source_table, source_row["__row_id"]))
            if not source_node_id:
                continue
            for target_node_id in target_lookup.get(key, []):
                if source_node_id == target_node_id:
                    continue
                pair = tuple(sorted((source_node_id, target_node_id)) + [edge_label])
                if pair in edge_seen:
                    continue
                edge_seen.add(pair)
                edges.append(
                    {
                        "id": f"edge:{len(edges) + 1}",
                        "source": source_node_id,
                        "target": target_node_id,
                        "label": edge_label,
                        "strength": 1.0,
                    }
                )
                node_details[source_node_id]["connections"] += 1
                node_details[target_node_id]["connections"] += 1

    connect_group_edges("products", "productGroup", "productGroup", "productGroup")
    connect_group_edges("sales_order_items", "materialGroup", "materialGroup", "materialGroup")
    connect_compound_edges(
        "sales_order_headers",
        ("salesOrder",),
        "sales_order_schedule_lines",
        ("salesOrder",),
        "salesOrder",
    )
    connect_compound_edges(
        "sales_order_items",
        ("salesOrder", "salesOrderItem"),
        "sales_order_schedule_lines",
        ("salesOrder", "salesOrderItem"),
        "salesOrderItem",
    )

    for link in inferred_links:
        if link["source_table"] not in GRAPH_CONFIG or link["target_table"] not in GRAPH_CONFIG:
            continue
        target_lookup: Dict[str, List[str]] = defaultdict(list)
        for target_row in conn.execute(
            f'SELECT "__row_id", "{link["target_field"]}" AS value FROM "{link["target_table"]}" '
            f'WHERE "{link["target_field"]}" IS NOT NULL AND TRIM("{link["target_field"]}") != ""'
        ):
            token = norm_id(target_row["value"])
            if not token or token in GENERIC_TOKENS:
                continue
            target_node_id = row_to_node_id.get((link["target_table"], target_row["__row_id"]))
            if target_node_id:
                target_lookup[token].append(target_node_id)

        edge_count_for_link = 0
        for source_row in conn.execute(
            f'SELECT "__row_id", "{link["source_field"]}" AS value FROM "{link["source_table"]}" '
            f'WHERE "{link["source_field"]}" IS NOT NULL AND TRIM("{link["source_field"]}") != ""'
        ):
            token = norm_id(source_row["value"])
            if not token or token in GENERIC_TOKENS:
                continue
            source_node_id = row_to_node_id.get((link["source_table"], source_row["__row_id"]))
            if not source_node_id:
                continue
            for target_node_id in target_lookup.get(token, []):
                if source_node_id == target_node_id:
                    continue
                pair = tuple(sorted((source_node_id, target_node_id)) + [link["label"]])
                if pair in edge_seen:
                    continue
                edge_seen.add(pair)
                edges.append(
                    {
                        "id": f"edge:{len(edges) + 1}",
                        "source": source_node_id,
                        "target": target_node_id,
                        "label": link["label"],
                        "strength": link["overlap_ratio"],
                    }
                )
                node_details[source_node_id]["connections"] += 1
                node_details[target_node_id]["connections"] += 1
                edge_count_for_link += 1
                if edge_count_for_link >= GRAPH_EDGE_LIMIT_PER_LINK:
                    break
            if edge_count_for_link >= GRAPH_EDGE_LIMIT_PER_LINK:
                break

    for node in nodes:
        node["connections"] = node_details[node["id"]]["connections"]

    scatter_width = 4200
    scatter_height = 2600
    for node in nodes:
        token = sum((index + 1) * ord(char) for index, char in enumerate(node["id"]))
        x_seed = (token * 1103515245 + 12345) % 2147483647
        y_seed = (token * 48271 + 67891) % 2147483647
        x = (x_seed / 2147483647.0) * scatter_width - scatter_width / 2
        y = (y_seed / 2147483647.0) * scatter_height - scatter_height / 2
        node["position"] = {"x": round(x, 2), "y": round(y, 2)}

    counts = defaultdict(int)
    for node in nodes:
        counts[node["entity"]] += 1

    payload = {
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "nodeCount": len(nodes),
            "edgeCount": len(edges),
            "entityCounts": dict(sorted(counts.items())),
        },
    }
    return payload, {key: sorted(value) for key, value in value_index.items()}, node_details


def build_ui_graph_payload(graph_payload: Dict[str, Any]) -> Dict[str, Any]:
    nodes_by_entity: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for node in graph_payload["nodes"]:
        if node.get("entity") in UI_EXCLUDED_ENTITIES:
            continue
        nodes_by_entity[str(node.get("entity"))].append(dict(node))

    nodes: List[Dict[str, Any]] = []
    for entity, entity_nodes in nodes_by_entity.items():
        limit = UI_ENTITY_LIMITS.get(entity, min(len(entity_nodes), UI_DEFAULT_ENTITY_LIMIT))
        entity_nodes.sort(key=lambda item: (-int(item.get("connections", 0)), str(item.get("label", ""))))
        nodes.extend(entity_nodes[:limit])

    allowed_node_ids = {node["id"] for node in nodes}
    node_table_by_id = {str(node["id"]): str(node.get("table", "")) for node in nodes}
    canonical_links = {
        (str(link["source_table"]), str(link["target_table"]), str(link["label"]))
        for link in FIXED_PROCESS_LINKS
    }
    canonical_links.update(
        (str(link["target_table"]), str(link["source_table"]), str(link["label"]))
        for link in FIXED_PROCESS_LINKS
    )
    canonical_links.update(UI_SYNTHETIC_LINKS)
    canonical_links.update((target, source, label) for source, target, label in UI_SYNTHETIC_LINKS)
    edges = [
        edge
        for edge in graph_payload["edges"]
        if edge.get("source") in allowed_node_ids
        and edge.get("target") in allowed_node_ids
        and (
            (
                node_table_by_id.get(str(edge.get("source")), ""),
                node_table_by_id.get(str(edge.get("target")), ""),
                str(edge.get("label", "")),
            )
            in canonical_links
        )
    ]
    entity_to_hub: Dict[str, str] = {}
    for hub_name, entities in HUB_ENTITY_GROUPS:
        for entity in entities:
            entity_to_hub[entity] = hub_name

    grouped_nodes: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for node in nodes:
        grouped_nodes[entity_to_hub.get(str(node.get("entity")), "other")].append(node)

    for hub_name, hub_nodes in grouped_nodes.items():
        hub_nodes.sort(
            key=lambda item: (
                HUB_ENTITY_PRIORITY.get(str(item.get("entity")), 3),
                -int(item.get("connections", 0)),
                str(item.get("label", "")),
            )
        )
        layout = UI_HUB_LAYOUTS.get(hub_name, UI_HUB_LAYOUTS["other"])
        center_x, center_y = layout["center"]
        tier_groups: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for node in hub_nodes:
            tier_groups[HUB_ENTITY_PRIORITY.get(str(node.get("entity")), 3)].append(node)

        start_deg, end_deg = layout["arc"]
        start_angle = math.radians(start_deg)
        end_angle = math.radians(end_deg)
        anchor = None
        if tier_groups.get(0):
            anchor = max(
                tier_groups[0],
                key=lambda item: (
                    int(item.get("connections", 0)),
                    len(item.get("details", [])),
                    str(item.get("label", "")),
                ),
            )
            anchor["position"] = {"x": round(center_x, 2), "y": round(center_y, 2)}

        tier_radius = layout["tier_radius"]
        x_scale = float(layout["x_scale"])
        y_scale = float(layout["y_scale"])
        spoke_count = max(int(layout.get("spokes", 12)), 1)
        spoke_gap = float(layout.get("spoke_gap", 18.0))
        for tier, tier_nodes in sorted(tier_groups.items()):
            ring_nodes = [node for node in tier_nodes if node is not anchor]
            if not ring_nodes:
                continue
            base_radius = tier_radius.get(tier, 260.0)
            span = end_angle - start_angle
            for index, node in enumerate(ring_nodes):
                spoke_index = index % spoke_count
                spoke_depth = index // spoke_count
                step_count = max(spoke_count - 1, 1)
                ratio = 0.5 if spoke_count == 1 else spoke_index / step_count
                angle = start_angle + (span * ratio)
                jitter_seed = sum((offset + 1) * ord(char) for offset, char in enumerate(str(node["id"])))
                radial_jitter = ((jitter_seed % 11) - 5) * 3.0
                angular_jitter = (((jitter_seed // 11) % 11) - 5) * 0.007
                radius = base_radius + (spoke_depth * spoke_gap) + radial_jitter
                x = center_x + math.cos(angle + angular_jitter) * radius * x_scale
                y = center_y + math.sin(angle + angular_jitter) * radius * y_scale
                node["position"] = {"x": round(x, 2), "y": round(y, 2)}
        if anchor is None and hub_nodes:
            hub_nodes[0]["position"] = {"x": round(center_x, 2), "y": round(center_y, 2)}

    entity_counts = defaultdict(int)
    for node in nodes:
        entity_counts[node["entity"]] += 1
    return {
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "nodeCount": len(nodes),
            "edgeCount": len(edges),
            "entityCounts": dict(sorted(entity_counts.items())),
        },
    }


def init_state() -> AppState:
    dataset_signature = compute_dataset_signature()
    cached_state = load_cached_graph_state(dataset_signature)
    if cached_state is not None:
        conn = build_sqlite_database(SQLITE_CACHE_PATH, force_rebuild=False)
        full_graph_payload = dict(cached_state["graph_payload"])
        return AppState(
            conn=conn,
            schema_text=str(cached_state["schema_text"]),
            graph_payload=full_graph_payload,
            ui_graph_payload=build_ui_graph_payload(full_graph_payload),
            value_index={str(key): list(value) for key, value in dict(cached_state["value_index"]).items()},
            node_details={str(key): dict(value) for key, value in dict(cached_state["node_details"]).items()},
            inferred_links=list(cached_state["inferred_links"]),
            adjacency={str(key): list(value) for key, value in dict(cached_state["adjacency"]).items()},
        )

    conn = build_sqlite_database(SQLITE_CACHE_PATH, force_rebuild=True)
    schema = fetch_table_schema(conn)
    inferred_links = infer_links(conn, schema)
    merged_links: List[Dict[str, Any]] = list(FIXED_PROCESS_LINKS)
    seen = {
        (link["source_table"], link["source_field"], link["target_table"], link["target_field"])
        for link in merged_links
    }
    for link in inferred_links:
        key = (link["source_table"], link["source_field"], link["target_table"], link["target_field"])
        reverse_key = (link["target_table"], link["target_field"], link["source_table"], link["source_field"])
        if key in seen or reverse_key in seen:
            continue
        merged_links.append(link)
        seen.add(key)
    schema_text = schema_to_text(schema, merged_links)
    graph_payload, value_index, node_details = build_graph(conn, merged_links)
    adjacency: Dict[str, List[str]] = defaultdict(list)
    for edge in graph_payload["edges"]:
        adjacency[edge["source"]].append(edge["target"])
        adjacency[edge["target"]].append(edge["source"])
    normalized_adjacency = {key: sorted(set(value)) for key, value in adjacency.items()}
    save_cached_graph_state(
        dataset_signature,
        {
            "schema_text": schema_text,
            "graph_payload": graph_payload,
            "value_index": value_index,
            "node_details": node_details,
            "inferred_links": merged_links,
            "adjacency": normalized_adjacency,
        },
    )
    return AppState(
        conn=conn,
        schema_text=schema_text,
        graph_payload=graph_payload,
        ui_graph_payload=build_ui_graph_payload(graph_payload),
        value_index=value_index,
        node_details=node_details,
        inferred_links=merged_links,
        adjacency=normalized_adjacency,
    )
