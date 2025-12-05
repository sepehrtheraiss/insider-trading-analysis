# insider_loader.py
class InsiderLoader:
    """Loads insider transactions into the DB."""

    def __init__(self, db):
        self.db = db

    def load(self, rows):
        # TODO: SQL upsert logic
        pass
