#Translates cli commands into real Click commands
import click
from .cli_commands import COMMANDS


def register_command(root: click.Group, name: str, spec: dict):
    parts = name.split(".")
    top = parts[0]
    sub = parts[1] if len(parts) > 1 else None

    callback = spec["callback"]
    help_text = spec.get("help", "")
    options = spec.get("options", [])

    # the function Click actually calls
    def click_callback(**kwargs):
        return callback(**kwargs)

    cmd = click.Command(
        name=parts[-1],
        callback=click_callback,
        help=help_text,
    )

    # attach options
    for opt_spec in options:
        # allow either ("--opt", {...}) or ("-o", "--opt", {...})
        *flags, meta = opt_spec
        cmd.params.append(click.Option(flags, **meta))

    if sub:
        # ensure subgroup exists
        if top not in root.commands:
            root.add_command(click.Group(name=top))
        root.commands[top].add_command(cmd)
    else:
        root.add_command(cmd)


def build_cli() -> click.Group:
    cli = click.Group(help="Insider Trading Analysis CLI")
    for name, spec in COMMANDS.items():
        register_command(cli, name, spec)
    return cli
