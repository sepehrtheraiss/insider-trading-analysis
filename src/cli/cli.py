# entry point
from .cli_factory import build_cli

cli = build_cli()

if __name__ == "__main__":
    cli()
