from flask import Blueprint, request, jsonify
from business_logic.extraction_service import ExtractionService


job1_bp = Blueprint('job1', __name__)
extraction_service = ExtractionService()


@job1_bp.route('/', methods=['POST'])
def extract_sales():
    """
    Presentation layer - handles HTTP request/response for job1
    
    Expected JSON:
    {
        "raw_dir": "file_storage/raw",
        "date": "2022-08-09"
    }
    """
    try:
        data = request.get_json()
        raw_dir = data.get('raw_dir')
        date_str = data.get('date')
        
        # Validate input
        if not raw_dir or not date_str:
            return jsonify({
                "error": "Required parameters: raw_dir and date"
            }), 400
        
        # Execute business logic
        result = extraction_service.extract_and_save(raw_dir, date_str)
        
        return jsonify(result), 201
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
