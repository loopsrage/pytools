from typing import Any, Dict, List
from optuna.storages.journal import BaseJournalBackend
from pymongo import MongoClient

class MongoJournalBackend(BaseJournalBackend):
    def __init__(self, mongo_url: str, db_name: str, collection_name: str):
        self.client = MongoClient(mongo_url)
        self.collection = self.client[db_name][collection_name]

    def append_logs(self, logs: List[Dict[str, Any]]) -> None:
        current_count = self.get_next_log_id()
        for i, log in enumerate(logs):
            self.collection.insert_one({"log_id": current_count + i, "data": log})

    def read_logs(self, log_number: int) -> List[Dict[str, Any]]:
        cursor = self.collection.find({"log_id": {"$gte": log_number}}).sort("log_id", 1)
        return [doc["data"] for doc in cursor]

    def get_next_log_id(self) -> int:
        return self.collection.count_documents({})