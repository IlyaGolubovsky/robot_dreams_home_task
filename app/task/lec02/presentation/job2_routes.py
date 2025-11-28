from flask import Blueprint, request, jsonify
from business_logic.avro_transformer import AvroTransformer

job2_bp = Blueprint('job2', __name__)
avro_transformer = AvroTransformer()


@job2_bp.route('/', methods=['POST'])
def transform_to_avro():
    """
    Presentation layer - handles HTTP request/response for job2

    Expected JSON:
    {
        "raw_dir": "file_storage/raw/sales/2022-08-09",
        "stg_dir": "file_storage/stg/sales/2022-08-09"
    }
    """
    try:
        data = request.get_json()
        raw_dir = data.get('raw_dir')
        stg_dir = data.get('stg_dir')

        # Validate input
        if not raw_dir or not stg_dir:
            return jsonify({
                "error": "Required parameters: raw_dir and stg_dir"
            }), 400

        # Execute business logic
        result = avro_transformer.transform_to_avro(raw_dir, stg_dir)

        return jsonify(result), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500
