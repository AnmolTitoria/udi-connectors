import pytest
from udi_connectors.dag_compiler import DagCompileError, compile_pipeline
from udi_connectors.pipeline import RuleTransform, SqlTransform


def node(id, type, config=None):
    return {"id": id, "type": type, "position": {"x": 0, "y": 0}, "config": config or {}}


def edge(id, source, target, target_handle=None):
    return {"id": id, "source": source, "target": target, "source_handle": None, "target_handle": target_handle}


class TestDirectPassthrough:
    def test_lands_and_publishes_under_destination_table_name(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "connector_type": "postgresql", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_published", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        compiled = compile_pipeline(definition)

        assert len(compiled.land_stages) == 1
        assert compiled.land_stages[0].connection_id == "conn-1"
        assert compiled.land_stages[0].table_name == "orders"

        assert len(compiled.transform_stages) == 1
        stage = compiled.transform_stages[0]
        assert stage.transform is None
        assert stage.source_type == "s3"
        assert stage.source_table_name == "orders"
        assert stage.output_table_name == "orders_published"

        assert compiled.publish_stages[0].table_name == "orders_published"


class TestMapOnlyChain:
    def test_compiles_to_rule_transform(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("map1", "map", {"rename": '{"amt": "amount"}', "drop_columns": "internal_id, temp"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_clean", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "map1"), edge("e2", "map1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        stage = compiled.transform_stages[0]

        assert isinstance(stage.transform, RuleTransform)
        assert stage.transform.rename == {"amt": "amount"}
        assert stage.transform.drop_columns == ["internal_id", "temp"]
        assert stage.source_type == "s3"

    def test_rename_json_dict_passed_directly_without_string_encoding(self):
        """config values arrive as whatever JSON the frontend saved — a
        dict, not just a string, is equally valid input."""
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("map1", "map", {"rename": {"amt": "amount"}}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_clean", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "map1"), edge("e2", "map1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        assert compiled.transform_stages[0].transform.rename == {"amt": "amount"}


class TestFilterChain:
    def test_compiles_to_sql_transform(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("filt1", "filter", {"predicate": "amount > 100"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "big_orders", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "filt1"), edge("e2", "filt1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        stage = compiled.transform_stages[0]

        assert isinstance(stage.transform, SqlTransform)
        assert stage.source_type == "athena"
        assert 'FROM "orders"' in stage.transform.sql
        assert "WHERE amount > 100" in stage.transform.sql

    def test_missing_predicate_raises(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("filt1", "filter", {}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "t", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "filt1"), edge("e2", "filt1", "dst1")],
        }
        with pytest.raises(DagCompileError, match="no predicate configured"):
            compile_pipeline(definition)


class TestJoinChain:
    def test_compiles_sql_referencing_both_tables(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("src2", "source", {"connection_id": "conn-2", "table_name": "customers"}),
                node("join1", "join", {"join_type": "left", "left_key": "customer_id", "right_key": "customer_id"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "enriched", "bucket_name": "b"}),
            ],
            "edges": [
                edge("e1", "src1", "join1", target_handle="left"),
                edge("e2", "src2", "join1", target_handle="right"),
                edge("e3", "join1", "dst1"),
            ],
        }
        compiled = compile_pipeline(definition)

        assert len(compiled.land_stages) == 2
        sql = compiled.transform_stages[0].transform.sql
        assert 'FROM "orders" l LEFT JOIN "customers" r ON l."customer_id" = r."customer_id"' in sql

    def test_compiles_sql_with_differently_named_keys(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("src2", "source", {"connection_id": "conn-2", "table_name": "customers"}),
                node("join1", "join", {"join_type": "inner", "left_key": "customer_id", "right_key": "id"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "enriched", "bucket_name": "b"}),
            ],
            "edges": [
                edge("e1", "src1", "join1", target_handle="left"),
                edge("e2", "src2", "join1", target_handle="right"),
                edge("e3", "join1", "dst1"),
            ],
        }
        compiled = compile_pipeline(definition)

        sql = compiled.transform_stages[0].transform.sql
        assert 'FROM "orders" l INNER JOIN "customers" r ON l."customer_id" = r."id"' in sql

    def test_join_requires_exactly_two_sources(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("join1", "join", {"join_type": "inner", "left_key": "id", "right_key": "id"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "t", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "join1", target_handle="left"), edge("e2", "join1", "dst1")],
        }
        with pytest.raises(DagCompileError, match="exactly 2 incoming edges"):
            compile_pipeline(definition)

    def test_join_inputs_must_be_source_nodes(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("filt1", "filter", {"predicate": "1=1"}),
                node("src2", "source", {"connection_id": "conn-2", "table_name": "customers"}),
                node("join1", "join", {"join_type": "inner", "left_key": "id", "right_key": "id"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "t", "bucket_name": "b"}),
            ],
            "edges": [
                edge("e1", "src1", "filt1"),
                edge("e2", "filt1", "join1", target_handle="left"),
                edge("e3", "src2", "join1", target_handle="right"),
                edge("e4", "join1", "dst1"),
            ],
        }
        with pytest.raises(DagCompileError, match="must come directly from source nodes"):
            compile_pipeline(definition)

    def test_mismatched_key_column_counts_raises(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("src2", "source", {"connection_id": "conn-2", "table_name": "customers"}),
                node("join1", "join", {"join_type": "inner", "left_key": "a,b", "right_key": "a"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "t", "bucket_name": "b"}),
            ],
            "edges": [
                edge("e1", "src1", "join1", target_handle="left"),
                edge("e2", "src2", "join1", target_handle="right"),
                edge("e3", "join1", "dst1"),
            ],
        }
        with pytest.raises(DagCompileError, match="same number of columns"):
            compile_pipeline(definition)


class TestAggregateChain:
    def test_compiles_group_by_and_aggregations(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("agg1", "aggregate", {"group_by": "region", "aggregations": '[{"column":"amount","fn":"sum","as":"total"}]'}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "by_region", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "agg1"), edge("e2", "agg1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        sql = compiled.transform_stages[0].transform.sql

        assert 'GROUP BY "region"' in sql
        assert 'SUM("amount") AS "total"' in sql

    def test_missing_aggregations_raises(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("agg1", "aggregate", {"group_by": "region"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "t", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "agg1"), edge("e2", "agg1", "dst1")],
        }
        with pytest.raises(DagCompileError, match="no aggregations configured"):
            compile_pipeline(definition)


class TestStructuralErrors:
    def test_cycle_detected(self):
        definition = {
            "nodes": [node("a", "filter", {"predicate": "1=1"}), node("b", "filter", {"predicate": "1=1"})],
            "edges": [edge("e1", "a", "b"), edge("e2", "b", "a")],
        }
        with pytest.raises(DagCompileError, match="cycle"):
            compile_pipeline(definition)

    def test_no_source_raises(self):
        definition = {
            "nodes": [node("dst1", "destination", {"connector_type": "s3", "table_name": "t", "bucket_name": "b"})],
            "edges": [],
        }
        with pytest.raises(DagCompileError, match="no source node"):
            compile_pipeline(definition)

    def test_no_destination_raises(self):
        definition = {
            "nodes": [node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"})],
            "edges": [],
        }
        with pytest.raises(DagCompileError, match="no destination node"):
            compile_pipeline(definition)

    def test_empty_pipeline_raises(self):
        with pytest.raises(DagCompileError, match="no nodes"):
            compile_pipeline({"nodes": [], "edges": []})

    def test_destination_missing_table_name_raises(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        with pytest.raises(DagCompileError, match="missing table_name"):
            compile_pipeline(definition)

    def test_non_s3_destination_rejected(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "postgresql", "table_name": "t"}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        with pytest.raises(DagCompileError, match="only 's3' is a registered destination"):
            compile_pipeline(definition)


class TestMergeKeys:
    def test_merge_keys_split_and_removed_from_target_config(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_published", "bucket_name": "b", "merge_keys": "id, updated_at"}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        publish = compiled.publish_stages[0]

        assert publish.merge_keys == ["id", "updated_at"]
        assert "merge_keys" not in publish.target_config

    def test_no_merge_keys_defaults_to_none(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_published", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        assert compiled.publish_stages[0].merge_keys is None

    def test_blank_merge_keys_defaults_to_none(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_published", "bucket_name": "b", "merge_keys": "  "}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        assert compiled.publish_stages[0].merge_keys is None


class TestDestinationConnectionId:
    def test_connection_id_extracted_and_removed_from_target_config(self):
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_published", "connection_id": "dest-conn-1"}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        publish = compiled.publish_stages[0]

        assert publish.destination_connection_id == "dest-conn-1"
        assert "connection_id" not in publish.target_config

    def test_no_connection_id_defaults_to_none_and_keeps_raw_target_config(self):
        """Older pipeline definitions embed connector config directly on the
        destination node instead of referencing a saved connection — that
        fallback still compiles, so existing saved pipelines keep working."""
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_published", "bucket_name": "b"}),
            ],
            "edges": [edge("e1", "src1", "dst1")],
        }
        compiled = compile_pipeline(definition)
        publish = compiled.publish_stages[0]

        assert publish.destination_connection_id is None
        assert publish.target_config == {"bucket_name": "b"}


class TestMultipleDestinations:
    def test_same_source_different_published_names(self):
        """One source feeding two destinations with different transforms
        publishes each under its own table_name without the two runs
        colliding."""
        definition = {
            "nodes": [
                node("src1", "source", {"connection_id": "conn-1", "table_name": "orders"}),
                node("dst1", "destination", {"connector_type": "s3", "table_name": "orders_raw", "bucket_name": "b"}),
                node("map1", "map", {"drop_columns": "internal_id"}),
                node("dst2", "destination", {"connector_type": "s3", "table_name": "orders_clean", "bucket_name": "b"}),
            ],
            "edges": [
                edge("e1", "src1", "dst1"),
                edge("e2", "src1", "map1"),
                edge("e3", "map1", "dst2"),
            ],
        }
        compiled = compile_pipeline(definition)

        assert len(compiled.land_stages) == 1
        assert len(compiled.transform_stages) == 2
        output_names = {s.output_table_name for s in compiled.transform_stages}
        assert output_names == {"orders_raw", "orders_clean"}
