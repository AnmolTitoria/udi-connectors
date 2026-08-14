from udi_connectors._registry import (
    create_source,
    create_target,
    get_source_class,
    get_source_meta,
    get_target_class,
    get_target_meta,
    list_sources,
    list_targets,
    load_plugin_dir,
    load_plugins,
    migrate_all,
    register_source,
    register_target,
)

import udi_connectors.postgresql
import udi_connectors.mongodb
import udi_connectors.sql
import udi_connectors.s3
import udi_connectors.file_upload
import udi_connectors.athena

from udi_connectors.pipeline import (
    PublishResult,
    RuleTransform,
    SqlTransform,
    Transform,
    migrate_raw_to_curated,
    publish_curated,
)

from udi_connectors.dag_compiler import (
    CompiledPipeline,
    DagCompileError,
    LandStage,
    PublishStage,
    TransformStage,
    compile_pipeline,
)

# Custom connectors: a packaged plugin registered under the
# "udi_connectors.plugins" entry-point group, or a .py file dropped in
# $UDI_CONNECTOR_PLUGINS_DIR — either way, it shows up in list_sources()/
# list_targets() and flows through create_source()/migrate_all() exactly
# like postgresql/mongodb/sql/... above, since it's the same _sources /
# _targets registry either way.
loaded_plugins = load_plugins() + load_plugin_dir()

__all__ = [
    "create_source",
    "create_target",
    "get_source_class",
    "get_target_class",
    "get_source_meta",
    "get_target_meta",
    "register_source",
    "register_target",
    "list_sources",
    "list_targets",
    "load_plugins",
    "load_plugin_dir",
    "loaded_plugins",
    "migrate_all",
    "migrate_raw_to_curated",
    "publish_curated",
    "SqlTransform",
    "RuleTransform",
    "Transform",
    "PublishResult",
    "compile_pipeline",
    "CompiledPipeline",
    "DagCompileError",
    "LandStage",
    "TransformStage",
    "PublishStage",
]
