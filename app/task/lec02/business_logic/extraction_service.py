import os
from data_access.api_client import SalesApiClient
from data_access.file_storage import FileStorage


class ExtractionService:
    """Business logic layer - handles sales data extraction"""
    
    def __init__(self):
        self.api_client = SalesApiClient()
        self.file_storage = FileStorage()
    
    def extract_and_save(self, raw_dir: str, date_str: str) -> dict:
        """
        Extract sales from API and save to file
        
        Args:
            raw_dir: Raw directory path
            date_str: Date in format 'YYYY-MM-DD'
            
        Returns:
            Dictionary with status and record count
        """
        # Prepare directory
        sales_dir = os.path.join(raw_dir, 'sales', date_str)
        self.file_storage.ensure_directory(sales_dir)
        
        # Fetch data from API
        records = self.api_client.fetch_sales(date_str)
        
        # Save to file
        file_path = os.path.join(sales_dir, f"sales_{date_str}.json")
        self.file_storage.save_json(records, file_path)
        
        return {
            "status": "success",
            "records": len(records),
            "file": file_path
        }
