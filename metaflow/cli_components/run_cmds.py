"""Run and resume CLI commands."""

import json
import os
import sys
import time

import click

from ..parameters import Parameter, JSONType
from ..user_configs.config_parameters import Config
from ..includefile import IncludeFile


# Module-level 'run' command for introspection by test runner / click_api
@click.command("run")
@click.option("--run-id-file", default=None, type=str)
@click.option("--max-workers", default=16, type=int)
@click.option("--max-num-splits", default=100, type=int)
@click.option("--tag", multiple=True, type=str)
@click.option("--config-value", multiple=True, nargs=2, type=str)
@click.option("--config", "config_file", multiple=True, nargs=2, type=str)
@click.pass_context
def run(ctx, **kwargs):
    """Run the flow."""
    pass


# Module-level 'resume' command for introspection
@click.command("resume")
@click.argument("step_to_rerun", required=False, default=None)
@click.option("--origin-run-id", default=None, type=str)
@click.option("--run-id-file", default=None, type=str)
@click.option("--tag", multiple=True, type=str)
@click.option("--max-workers", default=16, type=int)
@click.option("--max-num-splits", default=100, type=int)
@click.option("--config-value", multiple=True, nargs=2, type=str)
@click.option("--config", "config_file", multiple=True, nargs=2, type=str)
@click.pass_context
def resume(ctx, **kwargs):
    """Resume a flow from a failed step."""
    pass


def make_run_cmd(flow_cls):
    """Create the 'run' CLI command for a flow class."""

    @click.command("run")
    @click.option("--run-id-file", default=None, type=str)
    @click.option("--max-workers", default=16, type=int)
    @click.option("--max-num-splits", default=100, type=int)
    @click.option("--tag", multiple=True, type=str)
    @click.option("--config-value", multiple=True, nargs=2, type=str)
    @click.option("--config", "config_file", multiple=True, nargs=2, type=str)
    @click.pass_context
    def run_cmd(ctx, run_id_file, max_workers, max_num_splits, tag,
                config_value, config_file, **kwargs):
        """Run the flow."""
        from ..graph import FlowGraph
        from ..datastore.local import LocalDatastore
        from ..plugins.metadata_providers.local import LocalMetadataProvider
        from ..runtime import Runtime
        from ..decorators import ProjectDecorator

        # Set config CLI values
        _set_cli_configs_from_opts(config_value, config_file)

        # Set parameter values from CLI
        _set_cli_params(flow_cls, kwargs)

        # Generate run ID
        run_id = str(int(time.time() * 1000000))

        # Write run ID to file if requested
        if run_id_file:
            with open(run_id_file, "w") as f:
                f.write(run_id)

        graph = FlowGraph(flow_cls)
        ds = LocalDatastore()
        meta = LocalMetadataProvider()

        # Handle flow-level decorators
        username = os.environ.get("METAFLOW_USER", os.environ.get("USER", "unknown"))
        sys_tags = [
            "user:%s" % username,
            "runtime:dev",
        ]

        # Apply flow decorators
        flow_decos = getattr(flow_cls, "_flow_decorators", [])
        for deco in flow_decos:
            if isinstance(deco, ProjectDecorator):
                deco.flow_init(flow_cls, graph, None, None, None)
                if hasattr(flow_cls, "_project_name"):
                    sys_tags.append("project:%s" % flow_cls._project_name)
                    sys_tags.append("project_branch:%s" % flow_cls._branch_name)
                    if flow_cls._is_production:
                        sys_tags.append("production:True")

        user_tags = list(tag)

        runtime = Runtime(
            flow_cls, graph, ds, meta, run_id,
            tags=user_tags, sys_tags=sys_tags,
            max_workers=max_workers, max_num_splits=max_num_splits,
        )

        try:
            runtime.execute()
        except Exception as e:
            meta.done_run(flow_cls.__name__, run_id)
            raise

    # Add parameter options dynamically
    _add_param_options(run_cmd, flow_cls)

    return run_cmd


def make_resume_cmd(flow_cls):
    """Create the 'resume' CLI command."""

    @click.command("resume")
    @click.argument("step_to_rerun", required=False, default=None)
    @click.option("--origin-run-id", default=None, type=str)
    @click.option("--run-id-file", default=None, type=str)
    @click.option("--tag", multiple=True, type=str)
    @click.option("--max-workers", default=16, type=int)
    @click.option("--max-num-splits", default=100, type=int)
    @click.option("--config-value", multiple=True, nargs=2, type=str)
    @click.option("--config", "config_file", multiple=True, nargs=2, type=str)
    @click.pass_context
    def resume_cmd(ctx, step_to_rerun, origin_run_id, run_id_file, tag,
                   max_workers, max_num_splits, config_value, config_file, **kwargs):
        """Resume a flow from a failed step."""
        from ..graph import FlowGraph
        from ..datastore.local import LocalDatastore
        from ..plugins.metadata_providers.local import LocalMetadataProvider
        from ..runtime import Runtime
        from ..decorators import ProjectDecorator

        # Set configs
        _set_cli_configs_from_opts(config_value, config_file)

        # Set params
        _set_cli_params(flow_cls, kwargs)

        graph = FlowGraph(flow_cls)
        ds = LocalDatastore()
        meta = LocalMetadataProvider()
        flow_name = flow_cls.__name__

        # Find origin run
        if not origin_run_id:
            run_ids = meta.get_run_ids(flow_name)
            if run_ids:
                origin_run_id = run_ids[0]
            else:
                raise click.ClickException("No previous runs found to resume from")

        # Get tags from origin run
        origin_meta = meta.get_run_meta(flow_name, origin_run_id)
        user_tags = list(tag) if tag else list(origin_meta.get("tags", []))
        sys_tags = list(origin_meta.get("sys_tags", []))

        # Apply flow decorators
        flow_decos = getattr(flow_cls, "_flow_decorators", [])
        for deco in flow_decos:
            if isinstance(deco, ProjectDecorator):
                deco.flow_init(flow_cls, graph, None, None, None)

        # Generate new run ID
        run_id = str(int(time.time() * 1000000))
        if run_id_file:
            with open(run_id_file, "w") as f:
                f.write(run_id)

        # Find resume step
        if not step_to_rerun:
            step_names = meta.get_step_names(flow_name, origin_run_id)
            for node in graph:
                if node.name not in step_names:
                    step_to_rerun = node.name
                    break
                task_ids = meta.get_task_ids(flow_name, origin_run_id, node.name)
                all_ok = True
                for tid in task_ids:
                    arts = ds.load_artifacts(flow_name, origin_run_id, node.name, tid)
                    if not arts.get("_task_ok", False):
                        all_ok = False
                        break
                if not all_ok:
                    step_to_rerun = node.name
                    break
            if not step_to_rerun:
                step_to_rerun = "start"

        runtime = Runtime(
            flow_cls, graph, ds, meta, run_id,
            tags=user_tags, sys_tags=sys_tags,
            max_workers=max_workers, max_num_splits=max_num_splits,
            origin_run_id=origin_run_id,
        )

        try:
            runtime.execute(resume_step=step_to_rerun)
        except Exception as e:
            meta.done_run(flow_name, run_id)
            raise

    _add_param_options(resume_cmd, flow_cls)

    return resume_cmd


def _add_param_options(cmd, flow_cls):
    """Add click options for each Parameter on the flow."""
    for attr_name in dir(flow_cls):
        obj = getattr(flow_cls, attr_name, None)
        if isinstance(obj, Parameter):
            option_name = "--%s" % obj._attr_name.replace("_", "-")
            param_type = click.STRING
            if obj.type == int:
                param_type = click.INT
            elif obj.type == float:
                param_type = click.FLOAT
            elif obj.type == bool:
                param_type = click.BOOL

            cmd = click.option(
                option_name,
                default=None,
                type=param_type,
                help=obj.help or "",
            )(cmd)
        elif isinstance(obj, IncludeFile):
            option_name = "--%s" % obj._attr_name.replace("_", "-")
            cmd = click.option(
                option_name,
                default=None,
                type=click.STRING,
                help=obj.help or "",
            )(cmd)


def _set_cli_params(flow_cls, kwargs):
    """Set parameter values from CLI kwargs into env vars."""
    for attr_name in dir(flow_cls):
        obj = getattr(flow_cls, attr_name, None)
        if isinstance(obj, (Parameter, IncludeFile)):
            val = kwargs.get(obj._attr_name)
            if val is not None:
                env_key = "METAFLOW_RUN_%s" % obj._attr_name.upper()
                os.environ[env_key] = str(val)


def _set_cli_configs_from_opts(config_value_pairs, config_file_pairs):
    """Set config values from CLI options into env vars."""
    cli_values = {}
    cli_files = {}

    for name, val in (config_value_pairs or []):
        cli_values[name] = val
    for name, path in (config_file_pairs or []):
        cli_files[name] = path

    if cli_values:
        os.environ["_METAFLOW_CLI_CONFIG_VALUE"] = json.dumps(cli_values)
    if cli_files:
        os.environ["_METAFLOW_CLI_CONFIG"] = json.dumps(cli_files)
