import os
from fastavro import schemaless_writer
from config import AVRO_SCHEMA
from data_access.file_storage import FileStorage


class AvroTransformer:
    """Business logic layer - handles data transformation to Avro"""
    
    def __init__(self):
        self.schema = AVRO_SCHEMA
        self.file_storage = FileStorage()
    
    def convert_record(self, record: dict) -> dict:
        """
        Convert record according to Avro schema
        
        Args:
            record: Source record
            
        Returns:
            Converted record
        """
        converted = {}
        
        for field in self.schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]
            value = record.get(field_name)
            
            if value is None:
                converted[field_name] = None
            elif "int" in str(field_type):
                try:
                    converted[field_name] = int(value)
                except (ValueError, TypeError):
                    converted[field_name] = None
            elif "double" in str(field_type):
                try:
                    converted[field_name] = float(value)
                except (ValueError, TypeError):
                    converted[field_name] = None
            else:
                converted[field_name] = str(value) if value is not None else None
        
        return converted
    
    def transform_to_avro(self, raw_dir: str, stg_dir: str) -> dict:
        """
        Transform JSON files to Avro format
        
        Args:
            raw_dir: Directory with source JSON files
            stg_dir: Target directory for Avro files
            
        Returns:
            Dictionary with status and record count
        """
        # Prepare directory
        self.file_storage.ensure_directory(stg_dir)
        
        # Find JSON files
        json_files = self.file_storage.find_json_files(raw_dir)
        
        if not json_files:
            raise Exception(f"No JSON files found in {raw_dir}")
        
        total_records = 0
        
        # Process each JSON file
        for json_file in json_files:
            records = self.file_storage.load_json(json_file)
            
            # Create Avro file
            base_name = os.path.basename(json_file).replace('.json', '.avro')
            avro_file = os.path.join(stg_dir, base_name)
            
            with open(avro_file, 'wb') as f:
                for record in records:
                    converted = self.convert_record(record)
                    schemaless_writer(f, self.schema, converted)
                    total_records += 1
        
        return {
            "status": "success",
            "records": total_records,
            "directory": stg_dir
        }
