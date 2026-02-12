"""Top-level CLI for Metaflow flows."""

import json
import os
import pickle
import sys

import click


@click.group()
@click.option("--metadata", default="local", type=str)
@click.option("--datastore", default="local", type=str)
@click.option("--environment", default="local", type=str)
@click.option("--event-logger", default="nullSidecarLogger", type=str)
@click.option("--no-pylint", is_flag=True, default=False)
@click.option("--pylint", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--no-quiet", is_flag=True, default=False)
@click.option("--with", "with_", multiple=True, type=str)
@click.pass_context
def start(ctx, metadata, datastore, environment, event_logger,
          no_pylint, pylint, quiet, no_quiet, with_):
    """Metaflow flow runner."""
    ctx.ensure_object(dict)
    ctx.obj["metadata"] = metadata
    ctx.obj["datastore"] = datastore
    ctx.obj["environment"] = environment
    ctx.obj["quiet"] = quiet and not no_quiet
    ctx.obj["with_deco"] = list(with_)


def create_cli(flow_cls):
    """Build and execute CLI for a flow class."""
    from .cli_components.run_cmds import make_run_cmd, make_resume_cmd

    # Add subcommands
    run_cmd = make_run_cmd(flow_cls)
    resume_cmd = make_resume_cmd(flow_cls)

    start.add_command(run_cmd, "run")
    start.add_command(resume_cmd, "resume")

    # Add dump command
    @start.command()
    @click.option("--max-value-size", default=1000, type=int)
    @click.option("--private", is_flag=True, default=False)
    @click.option("--include", type=str, default=None)
    @click.option("--file", "output_file", type=str, default=None)
    @click.argument("pathspec")
    @click.pass_context
    def dump(ctx, max_value_size, private, include, output_file, pathspec):
        """Dump artifacts."""
        from .datastore.local import LocalDatastore
        from .plugins.metadata_providers.local import LocalMetadataProvider

        ds = LocalDatastore()
        meta = LocalMetadataProvider()

        parts = pathspec.split("/")
        flow_name = flow_cls.__name__

        if len(parts) == 2:
            run_id, step_name = parts
        elif len(parts) == 3:
            run_id, step_name, task_id_hint = parts
        else:
            run_id = parts[0]
            step_name = parts[1] if len(parts) > 1 else None

        task_ids = meta.get_task_ids(flow_name, run_id, step_name)
        result = {}

        for task_id in task_ids:
            artifacts = ds.load_artifacts(flow_name, run_id, step_name, task_id)
            filtered = {}
            for name, val in artifacts.items():
                if not private and name.startswith("_"):
                    continue
                if include and name != include:
                    continue
                filtered[name] = val
            key = "%s/%s/%s" % (run_id, step_name, task_id)
            result[key] = filtered

        if output_file:
            with open(output_file, "wb") as f:
                pickle.dump(result, f)
        else:
            for task_id, arts in result.items():
                for name, val in arts.items():
                    click.echo("%s: %s" % (name, repr(val)[:max_value_size]))

    # Add logs command
    @start.command()
    @click.option("--stdout", "logtype", flag_value="stdout")
    @click.option("--stderr", "logtype", flag_value="stderr")
    @click.argument("pathspec")
    @click.pass_context
    def logs(ctx, logtype, pathspec):
        """Show logs."""
        from .datastore.local import LocalDatastore
        from .plugins.metadata_providers.local import LocalMetadataProvider

        ds = LocalDatastore()
        meta = LocalMetadataProvider()

        flow_name = flow_cls.__name__
        parts = pathspec.split("/")
        run_id = parts[0]
        step_name = parts[1] if len(parts) > 1 else None

        if not logtype:
            logtype = "stdout"

        if step_name:
            task_ids = meta.get_task_ids(flow_name, run_id, step_name)
            for task_id in task_ids:
                log = ds.load_log(flow_name, run_id, step_name, task_id, logtype)
                click.echo(log, nl=False)

    # Add tag group
    @start.group()
    def tag():
        """Tag management."""
        pass

    @tag.command("list")
    @click.option("--flat", is_flag=True, default=False)
    @click.option("--hide-system-tags", is_flag=True, default=False)
    @click.option("--run-id", required=True, type=str)
    @click.pass_context
    def tag_list(ctx, flat, hide_system_tags, run_id):
        """List tags."""
        from .plugins.metadata_providers.local import LocalMetadataProvider

        meta = LocalMetadataProvider()
        flow_name = flow_cls.__name__
        run_meta = meta.get_run_meta(flow_name, run_id)

        if run_meta:
            all_tags = set(run_meta.get("tags", []))
            sys_tags = set(run_meta.get("sys_tags", []))
            click.echo("Tags for run %s:" % run_id, err=True)
            if hide_system_tags:
                for t in sorted(all_tags):
                    click.echo(t, err=True)
            else:
                for t in sorted(all_tags | sys_tags):
                    click.echo(t, err=True)

    @tag.command("add")
    @click.option("--run-id", required=True, type=str)
    @click.argument("tags", nargs=-1)
    @click.pass_context
    def tag_add(ctx, run_id, tags):
        """Add tags."""
        from .plugins.metadata_providers.local import LocalMetadataProvider

        meta = LocalMetadataProvider()
        flow_name = flow_cls.__name__
        for t in tags:
            if not isinstance(t, str) or len(t) == 0:
                raise click.ClickException("Tag must be a non-empty string")
            if len(t) > 512:
                raise click.ClickException("Tag must not exceed 512 characters")
            try:
                t.encode("utf-8").decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                raise click.ClickException("Tag must be valid UTF-8")
        run_meta = meta.get_run_meta(flow_name, run_id)
        if run_meta:
            sys_tags = set(run_meta.get("sys_tags", []))
            existing = set(run_meta.get("tags", []))
            for t in tags:
                if t not in sys_tags:
                    existing.add(t)
            meta.update_run_tags(flow_name, run_id, tags=list(existing))

    @tag.command("remove")
    @click.option("--run-id", required=True, type=str)
    @click.argument("tags", nargs=-1)
    @click.pass_context
    def tag_remove(ctx, run_id, tags):
        """Remove tags."""
        from .plugins.metadata_providers.local import LocalMetadataProvider

        meta = LocalMetadataProvider()
        flow_name = flow_cls.__name__
        for t in tags:
            if not isinstance(t, str):
                raise click.ClickException("Tag must be a string")
        run_meta = meta.get_run_meta(flow_name, run_id)
        if run_meta:
            sys_tags = set(run_meta.get("sys_tags", []))
            for t in tags:
                if t in sys_tags:
                    raise click.ClickException("Cannot remove system tag '%s'" % t)
            existing = set(run_meta.get("tags", []))
            existing -= set(tags)
            meta.update_run_tags(flow_name, run_id, tags=list(existing))

    @tag.command("replace")
    @click.option("--run-id", required=True, type=str)
    @click.option("--remove", "remove_tags", multiple=True, type=str)
    @click.option("--add", "add_tags", multiple=True, type=str)
    @click.argument("positional_args", nargs=-1)
    @click.pass_context
    def tag_replace(ctx, run_id, remove_tags, add_tags, positional_args):
        """Replace tags."""
        from .plugins.metadata_providers.local import LocalMetadataProvider

        meta = LocalMetadataProvider()
        flow_name = flow_cls.__name__

        if positional_args and len(positional_args) == 2:
            pass  # positional: tag replace --run-id RID OLD NEW
        elif not remove_tags and not add_tags:
            raise click.ClickException(
                "Specify tags to --add and/or --remove, "
                "or provide two positional arguments (OLD NEW)"
            )

        # Validate no system tags in add
        for t in add_tags:
            if isinstance(t, bytes):
                raise click.ClickException("Tags must be strings, not bytes")

        run_meta = meta.get_run_meta(flow_name, run_id)
        if run_meta:
            existing = set(run_meta.get("tags", []))
            if positional_args and len(positional_args) == 2:
                existing.discard(positional_args[0])
                existing.add(positional_args[1])
            else:
                existing -= set(remove_tags)
                existing.update(add_tags)
            meta.update_run_tags(flow_name, run_id, tags=list(existing))

    # Add card group
    @start.group()
    def card():
        """Card management."""
        pass

    @card.command("get")
    @click.argument("pathspec")
    @click.argument("output_file")
    @click.option("--type", "card_type", default="default")
    @click.option("--hash", "card_hash", default=None)
    @click.option("--id", "card_id", default=None)
    @click.pass_context
    def card_get(ctx, pathspec, output_file, card_type, card_hash, card_id):
        """Get a card."""
        from .cards import get_cards
        from .plugins.cards.exception import CardNotPresentException

        flow_name = flow_cls.__name__
        full_pathspec = "%s/%s" % (flow_name, pathspec)

        try:
            cards = get_cards(full_pathspec, type=card_type, id=card_id)
            if not cards or len(cards) == 0:
                raise CardNotPresentException("No cards found")

            # Filter by hash if specified
            if card_hash is not None:
                matching = [c for c in cards if card_hash in c.hash]
                if not matching:
                    raise CardNotPresentException("No card with hash %s" % card_hash)
                card_data = matching[0].get()
            else:
                card_data = cards[0].get()

            with open(output_file, "w") as f:
                f.write(card_data if card_data else "")
        except CardNotPresentException as e:
            click.echo("%s: %s" % (CardNotPresentException.headline, str(e)), err=True)
            sys.exit(1)

    @card.command("list")
    @click.argument("pathspec")
    @click.option("--as-json", is_flag=True, default=False)
    @click.option("--file", "output_file", default=None)
    @click.option("--type", "card_type", default=None)
    @click.pass_context
    def card_list(ctx, pathspec, as_json, output_file, card_type):
        """List cards."""
        from .cards import get_cards
        from .plugins.cards.exception import CardNotPresentException

        flow_name = flow_cls.__name__
        full_pathspec = "%s/%s" % (flow_name, pathspec)

        try:
            cards = get_cards(full_pathspec, type=card_type)
            result = {"pathspec": full_pathspec, "cards": []}
            if cards and len(cards) > 0:
                result["cards"] = [
                    {
                        "hash": c.hash,
                        "id": c.id,
                        "type": c.type,
                        "filename": "%s.html" % c.hash,
                    }
                    for c in cards
                ]
        except CardNotPresentException:
            result = {"pathspec": full_pathspec, "cards": []}

        if output_file:
            with open(output_file, "w") as f:
                json.dump(result, f)
        elif as_json:
            click.echo(json.dumps(result))

    # Add show command
    @start.command()
    @click.pass_context
    def show(ctx):
        """Show flow graph."""
        from .graph import FlowGraph
        graph = FlowGraph(flow_cls)
        for node in graph:
            click.echo("%s -> %s" % (node.name, node.out_funcs))

    # Execute CLI
    start(standalone_mode=True)
