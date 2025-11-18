import os
import json
import shutil
import glob


class FileStorage:
    """Data access layer - handles file operations"""
    
    @staticmethod
    def ensure_directory(directory: str) -> None:
        """Create directory, removing if exists"""
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.makedirs(directory, exist_ok=True)
    
    @staticmethod
    def save_json(records: list, file_path: str) -> None:
        """Save records to JSON file"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
    
    @staticmethod
    def load_json(file_path: str) -> list:
        """Load records from JSON file"""
        with open(file_path, 'r', encoding='utf-8') as f:
            records = json.load(f)
        return records if isinstance(records, list) else [records]
    
    @staticmethod
    def find_json_files(directory: str) -> list:
        """Find all JSON files in directory"""
        return sorted(glob.glob(os.path.join(directory, '*.json')))
    
    @staticmethod
    def save_avro(file_path: str) -> None:
        """Create Avro file handler"""
        return open(file_path, 'wb')
