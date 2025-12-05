# db.py
class DB:
    """Thin wrapper around your SQL database."""

    def last_updated(self, table_name: str):
        # TODO: SELECT MAX(updated_at)
        return None

    def get_insider_data(self, ticker, start, end):
        # TODO: SELECT * FROM insider_transactions WHERE...
        return []
