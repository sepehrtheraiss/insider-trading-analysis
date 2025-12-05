from dotenv import load_dotenv
load_dotenv()  # loads .env from project root automatically

from orchestrator.pipeline import OrchestratorPipeline

if __name__ == "__main__":
    OrchestratorPipeline().run()

