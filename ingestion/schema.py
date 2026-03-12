import json
import uuid
import hashlib
from datetime import datetime
class Schema:
    def __init__(self, raw_data, source, entity, methods, http_status=200, search_params=None):
            self.raw_data = raw_data
            self.source = source
            self.entity = entity
            self.methods = methods
            self.http_status = http_status
            self.search_params = search_params
            self.ingestion_id = str(uuid.uuid4())
            self.ingestion_timestamp = datetime.now().isoformat()

    def _generate_hash(self):
        return hashlib.sha256(json.dumps(self.raw_data, sort_keys=True).encode()).hexdigest()
    
    def build_schema(self):
        schema = {
        "metadata": {
            "ingestion_id": self.ingestion_id,
            "source_system": self.source,
            "entity": self.entity,
            "extraction_method": self.methods,
            "ingestion_timestamp": self.ingestion_timestamp,
            "http_status": self.http_status,
            "search_parameters": self.search_params,
            "raw_hash": self._generate_hash()
        },
        "raw_payload": self.raw_data
        }
        return schema