# generation logic
import click
from config.insider_trading_config import InsiderTradingConfig
from controllers.core_controller import CoreController
from .cli_commands import COMMANDS


def get_controller():
    return CoreController(Config())


def require_outpath_if_saved(params):
    save = params.get("save")
    outpath = params.get("outpath")
    if save and not outpath:
        raise click.UsageError("--save requires --outpath.")
    return params


def register_command(cli, name, spec):
    """
    Auto-create a Click command from a declarative spec.
    """
    parts = name.split(".")
    top = parts[0]
    sub = parts[1] if len(parts) > 1 else None

    handler_name = spec["handler"]
    help_text = spec.get("help", "")

    # Resolve controller handler method
    controller = get_controller()
    handler_func = getattr(controller, handler_name)

    # Dynamically build the function body
    def command_body(**kwargs):
        params = require_outpath_if_saved(kwargs)
        handler_func(params)

    # Wrap into Click command
    cmd = click.Command(
        name=parts[-1],
        callback=command_body,
        help=help_text,
    )

    # Dynamically add options
    for opt, meta in spec["options"]:
        cmd.params.append(
            click.Option([opt], **meta)
        )

    # Attach command to either root or subgroup
    if sub:
        # ensure subgroup exists
        if top not in cli.commands:
            cli.add_command(click.Group(name=top))
        cli.commands[top].add_command(cmd)
    else:
        cli.add_command(cmd)


def build_cli():
    cli = click.Group(help="Insider Trading Analysis CLI")

    for name, spec in COMMANDS.items():
        register_command(cli, name, spec)

    return cli
