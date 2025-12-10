# cli entry point
from dotenv import load_dotenv
from cli.cli_factory import build_cli

load_dotenv()  # loads .env from project root automatically
cli = build_cli()

if __name__ == "__main__":
    cli()
