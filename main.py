from dotenv import load_dotenv
from orchestrator.pipeline import OrchestratorPipeline

load_dotenv()
if __name__ == "__main__":
    OrchestratorPipeline().run()

