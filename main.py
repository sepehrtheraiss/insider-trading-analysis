from dotenv import load_dotenv
from orchestrator.pipeline import OrchestratorPipeline

load_dotenv()  # loads .env from project root automatically
if __name__ == "__main__":
    OrchestratorPipeline().run()

