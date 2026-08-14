"""Compiles a Pipeline Designer DAG (the {nodes, edges} shape saved by
udi-etl-app's /pipelines endpoints) into a sequence of stages built from the
existing Stage 1/2/3 primitives in pipeline.py and _registry.py
(migrate_all, migrate_raw_to_curated, publish_curated) — this is a new way
to *assemble* that existing engine from a graph, not a new engine.

A transform chain feeding a destination compiles one of two ways:
  - map-only (rename/drop_columns, no filter/join/aggregate) -> a
    RuleTransform, which also works against the dependency-free direct S3
    reader (source_type="s3").
  - anything with filter/join/aggregate -> a generated SqlTransform run
    through Athena (source_type="athena"). This requires the connection's
    raw-zone data to already be registered as an Athena/Glue table outside
    this codebase — there is no auto-registration anywhere in this project
    (confirmed: no glue client usage exists anywhere), the same manual
    prerequisite the wizard's hand-written SQL transform step already has
    today. The generated SQL references tables by the exact `table_name`
    each source node is configured with — it is the caller's responsibility
    to ensure a crawler/manual Glue table produces that same name.

Join nodes are only supported with both inputs coming directly from source
nodes (not from another transform's output) — this keeps the compiler
tractable for v1 and covers the common star-join shape.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal

from .pipeline import RuleTransform, SqlTransform, Transform

_TRANSFORM_TYPES = {"filter", "map", "join", "aggregate"}


class DagCompileError(Exception):
    pass


@dataclass
class LandStage:
    """Stage 1: land one source node's table into the S3 raw zone."""

    node_id: str
    connection_id: str
    table_name: str


@dataclass
class TransformStage:
    """Stage 2: raw zone -> curated/_staging, feeding one destination node."""

    node_id: str  # the destination node this stage feeds
    transform_node_ids: list[str]
    primary_connection_id: str  # whose raw zone this stage reads from
    source_table_name: str  # only meaningful for source_type="s3" — SqlTransform SQL is self-contained
    output_table_name: str
    transform: Transform | None
    source_type: Literal["athena", "s3"]


@dataclass
class PublishStage:
    """Stage 3: curated/_staging -> curated/, for one destination node.

    `destination_connection_id`, when set, is a saved (encrypted) connection
    the runner resolves at execute time — the modern path, mirroring how
    source nodes reference connections instead of embedding credentials in
    the pipeline definition. `target_config` is the pre-existing fallback
    for destination nodes that still embed raw connector config directly
    (older pipeline definitions saved before that fix); a node has one or
    the other, never both.
    """

    node_id: str
    primary_connection_id: str
    table_name: str
    target_config: dict[str, Any] = field(default_factory=dict)
    merge_keys: list[str] | None = None
    destination_connection_id: str | None = None


@dataclass
class CompiledPipeline:
    land_stages: list[LandStage]
    transform_stages: list[TransformStage]
    publish_stages: list[PublishStage]
    node_order: list[str]


def _topo_order(node_ids: list[str], edges: list[dict]) -> list[str]:
    incoming_count = {nid: 0 for nid in node_ids}
    outgoing: dict[str, list[str]] = {}
    for e in edges:
        incoming_count[e["target"]] = incoming_count.get(e["target"], 0) + 1
        outgoing.setdefault(e["source"], []).append(e["target"])

    queue = [nid for nid, c in incoming_count.items() if c == 0]
    order: list[str] = []
    while queue:
        nid = queue.pop(0)
        order.append(nid)
        for nxt in outgoing.get(nid, []):
            incoming_count[nxt] -= 1
            if incoming_count[nxt] == 0:
                queue.append(nxt)

    if len(order) != len(node_ids):
        raise DagCompileError("Pipeline contains a cycle")
    return order


def _split_csv(value: Any) -> list[str]:
    if not value or not isinstance(value, str):
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_json_field(value: Any, node_id: str, field_name: str) -> Any:
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise DagCompileError(f"Node {node_id}: '{field_name}' is not valid JSON") from e
    return value


def _node_config(node: dict) -> dict[str, Any]:
    return dict(node.get("config") or {})


def _collect_chain(dest_id: str, incoming: dict[str, list[dict]], nodes: dict[str, dict]) -> tuple[list[str], list[dict]]:
    """Walks backward from a destination node, collecting the ordered chain
    of transform node ids feeding it and the source node(s) at its root.
    Every non-join node in the chain must have exactly one incoming edge;
    a join node must have exactly two, both from source nodes directly."""
    incoming_edges = incoming.get(dest_id, [])
    if not incoming_edges:
        return [], []
    if len(incoming_edges) > 1:
        raise DagCompileError(f"Node {dest_id} has multiple incoming edges but isn't a join")

    chain: list[str] = []
    sources: list[dict] = []
    cursor = incoming_edges[0]["source"]

    while True:
        node = nodes.get(cursor)
        if node is None:
            raise DagCompileError(f"Dangling edge to unknown node {cursor}")
        if node["type"] == "source":
            sources.append(node)
            break

        chain.append(node["id"])

        if node["type"] == "join":
            join_edges = incoming.get(node["id"], [])
            if len(join_edges) != 2:
                raise DagCompileError(f"Join node {node['id']} must have exactly 2 incoming edges")
            for e in sorted(join_edges, key=lambda e: e.get("target_handle") or ""):
                parent = nodes.get(e["source"])
                if parent is None or parent["type"] != "source":
                    raise DagCompileError(f"Join node {node['id']}: both inputs must come directly from source nodes")
                sources.append(parent)
            break

        parent_edges = incoming.get(node["id"], [])
        if len(parent_edges) != 1:
            raise DagCompileError(f"Node {node['id']} must have exactly one incoming edge")
        cursor = parent_edges[0]["source"]

    chain.reverse()
    return chain, sources


def _build_rule_transform(chain_nodes: list[dict]) -> RuleTransform:
    rename: dict[str, str] = {}
    drop_columns: list[str] = []
    for node in chain_nodes:
        cfg = _node_config(node)
        raw_rename = _parse_json_field(cfg.get("rename"), node["id"], "rename")
        if isinstance(raw_rename, dict):
            rename.update(raw_rename)
        drop_columns.extend(_split_csv(cfg.get("drop_columns")))
    return RuleTransform(rename=rename, drop_columns=drop_columns)


def _quote(col: str) -> str:
    return f'"{col}"'


def _build_sql(chain_nodes: list[dict], feeding_sources: list[dict]) -> str:
    ctes: list[str] = []
    remaining = chain_nodes

    if chain_nodes and chain_nodes[0]["type"] == "join":
        join_node = chain_nodes[0]
        cfg = _node_config(join_node)
        join_type = (cfg.get("join_type") or "inner").upper()
        # Separate left_key/right_key (not a single shared "keys" field) so a
        # table's primary key can join against a differently-named foreign
        # key on the other side — see the Join field spec in
        # transformFieldSpecs.ts. Comma-separated, matched positionally for
        # composite keys.
        left_keys = _split_csv(cfg.get("left_key"))
        right_keys = _split_csv(cfg.get("right_key"))
        if not left_keys or not right_keys:
            raise DagCompileError(f"Join node {join_node['id']} needs both a left and right key")
        if len(left_keys) != len(right_keys):
            raise DagCompileError(f"Join node {join_node['id']}: left_key and right_key must have the same number of columns")
        if len(feeding_sources) != 2:
            raise DagCompileError(f"Join node {join_node['id']} requires exactly 2 upstream sources")
        left_table = _node_config(feeding_sources[0]).get("table_name")
        right_table = _node_config(feeding_sources[1]).get("table_name")
        if not left_table or not right_table:
            raise DagCompileError(f"Join node {join_node['id']}: both upstream sources need a table_name")
        on_clause = " AND ".join(f"l.{_quote(lk)} = r.{_quote(rk)}" for lk, rk in zip(left_keys, right_keys))
        ctes.append(f"base AS (SELECT * FROM {_quote(left_table)} l {join_type} JOIN {_quote(right_table)} r ON {on_clause})")
        remaining = chain_nodes[1:]
    else:
        if len(feeding_sources) != 1:
            raise DagCompileError("A non-join transform chain must have exactly one upstream source")
        base_table = _node_config(feeding_sources[0]).get("table_name")
        if not base_table:
            raise DagCompileError("Upstream source needs a table_name")
        ctes.append(f"base AS (SELECT * FROM {_quote(base_table)})")

    prev = "base"
    for i, node in enumerate(remaining):
        cfg = _node_config(node)
        alias = f"stage_{i}"

        if node["type"] == "filter":
            predicate = cfg.get("predicate")
            if not predicate:
                raise DagCompileError(f"Filter node {node['id']} has no predicate configured")
            ctes.append(f"{alias} AS (SELECT * FROM {prev} WHERE {predicate})")

        elif node["type"] == "map":
            raw_rename = _parse_json_field(cfg.get("rename"), node["id"], "rename")
            rename = raw_rename if isinstance(raw_rename, dict) else {}
            drop_columns = _split_csv(cfg.get("drop_columns"))
            except_cols = list(rename.keys()) + drop_columns
            select_parts = [f"* EXCEPT ({', '.join(_quote(c) for c in except_cols)})"] if except_cols else ["*"]
            select_parts.extend(f"{_quote(old)} AS {_quote(new)}" for old, new in rename.items())
            ctes.append(f"{alias} AS (SELECT {', '.join(select_parts)} FROM {prev})")

        elif node["type"] == "aggregate":
            group_by = _split_csv(cfg.get("group_by"))
            raw_aggs = _parse_json_field(cfg.get("aggregations"), node["id"], "aggregations")
            if not raw_aggs:
                raise DagCompileError(f"Aggregate node {node['id']} has no aggregations configured")
            agg_exprs = [f'{agg["fn"].upper()}({_quote(agg["column"])}) AS {_quote(agg["as"])}' for agg in raw_aggs]
            select_list = [_quote(g) for g in group_by] + agg_exprs
            group_clause = f" GROUP BY {', '.join(_quote(g) for g in group_by)}" if group_by else ""
            ctes.append(f"{alias} AS (SELECT {', '.join(select_list)} FROM {prev}{group_clause})")

        elif node["type"] == "join":
            raise DagCompileError("Only one join node per transform chain is supported today")
        else:
            raise DagCompileError(f"Unsupported node type in transform chain: {node['type']}")

        prev = alias

    cte_sql = ",\n  ".join(ctes)
    return f"WITH\n  {cte_sql}\nSELECT * FROM {prev}"


def compile_pipeline(definition: dict) -> CompiledPipeline:
    nodes = {n["id"]: n for n in definition.get("nodes", [])}
    edges = definition.get("edges", [])
    if not nodes:
        raise DagCompileError("Pipeline has no nodes")

    node_order = _topo_order(list(nodes.keys()), edges)

    incoming: dict[str, list[dict]] = {}
    for e in edges:
        incoming.setdefault(e["target"], []).append(e)

    source_nodes = [n for n in nodes.values() if n["type"] == "source"]
    destination_nodes = [n for n in nodes.values() if n["type"] == "destination"]
    if not source_nodes:
        raise DagCompileError("Pipeline has no source node")
    if not destination_nodes:
        raise DagCompileError("Pipeline has no destination node")

    land_stages: list[LandStage] = []
    for src in source_nodes:
        cfg = _node_config(src)
        connection_id = cfg.get("connection_id")
        table_name = cfg.get("table_name")
        if not connection_id or not table_name:
            raise DagCompileError(f"Source node {src['id']} is missing connection_id/table_name")
        land_stages.append(LandStage(node_id=src["id"], connection_id=connection_id, table_name=table_name))

    transform_stages: list[TransformStage] = []
    publish_stages: list[PublishStage] = []

    for dest in destination_nodes:
        dest_cfg = _node_config(dest)
        dest_table_name = dest_cfg.pop("table_name", None)
        dest_connector_type = dest_cfg.pop("connector_type", None)
        dest_merge_keys = _split_csv(dest_cfg.pop("merge_keys", None)) or None
        dest_connection_id = dest_cfg.pop("connection_id", None)
        if not dest_table_name:
            raise DagCompileError(f"Destination node {dest['id']} is missing table_name")
        if dest_connector_type and dest_connector_type != "s3":
            raise DagCompileError(f"Destination node {dest['id']}: only 's3' is a registered destination today")

        chain_ids, feeding_sources = _collect_chain(dest["id"], incoming, nodes)
        if not feeding_sources:
            raise DagCompileError(f"Destination node {dest['id']} has no source feeding it")

        primary_connection_id = _node_config(feeding_sources[0]).get("connection_id")
        primary_table_name = _node_config(feeding_sources[0]).get("table_name")
        chain_nodes = [nodes[nid] for nid in chain_ids]

        # Stage 2/3 always run per destination, even with an empty chain
        # (transform=None is a straight passthrough) — this is what lets the
        # destination's own table_name be the real published name, distinct
        # from whatever the source's raw table_name is, and what lets two
        # destinations reading the same source publish under different
        # names without fighting over a single Stage 1 landing.
        if not chain_nodes:
            transform_stages.append(TransformStage(
                node_id=dest["id"],
                transform_node_ids=[],
                primary_connection_id=primary_connection_id,
                source_table_name=primary_table_name,
                output_table_name=dest_table_name,
                transform=None,
                source_type="s3",
            ))
        elif all(n["type"] == "map" for n in chain_nodes):
            transform_stages.append(TransformStage(
                node_id=dest["id"],
                transform_node_ids=chain_ids,
                primary_connection_id=primary_connection_id,
                source_table_name=primary_table_name,
                output_table_name=dest_table_name,
                transform=_build_rule_transform(chain_nodes),
                source_type="s3",
            ))
        else:
            transform_stages.append(TransformStage(
                node_id=dest["id"],
                transform_node_ids=chain_ids,
                primary_connection_id=primary_connection_id,
                source_table_name=primary_table_name,
                output_table_name=dest_table_name,
                transform=SqlTransform(sql=_build_sql(chain_nodes, feeding_sources)),
                source_type="athena",
            ))
        publish_stages.append(PublishStage(
            node_id=dest["id"],
            primary_connection_id=primary_connection_id,
            table_name=dest_table_name,
            target_config=dest_cfg,
            merge_keys=dest_merge_keys,
            destination_connection_id=dest_connection_id,
        ))

    return CompiledPipeline(
        land_stages=land_stages,
        transform_stages=transform_stages,
        publish_stages=publish_stages,
        node_order=node_order,
    )
