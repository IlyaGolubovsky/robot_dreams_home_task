import os
import json
import shutil
import glob
from flask import Flask, request, jsonify
from fastavro import schemaless_writer

"""
# Job 2 - конвертує в Avro
python main_avro.py

curl -X POST http://localhost:8082/ `
  -H "Content-Type: application/json" `
  -d '{"raw_dir":"file_storage/raw/sales/2022-08-09","stg_dir":"file_storage/stg/sales/2022-08-09"}'
"""

app = Flask(__name__)

AVRO_SCHEMA = {
    "type": "record",
    "name": "SalesRecord",
    "fields": [
        {"name": "client", "type": ["null", "string"], "default": None},
        {"name": "purchase_date", "type": ["null", "string"], "default": None},
        {"name": "product", "type": ["null", "string"], "default": None},
        {"name": "price", "type": ["null", "double"], "default": None},
    ]
}


def convert_record(record):
    """Конвертує запис відповідно до схеми"""
    converted = {}
    for field in AVRO_SCHEMA["fields"]:
        field_name = field["name"]
        field_type = field["type"]
        value = record.get(field_name)

        if value is None:
            converted[field_name] = None
        elif "int" in str(field_type):
            try:
                converted[field_name] = int(value)
            except:
                converted[field_name] = None
        elif "double" in str(field_type):
            try:
                converted[field_name] = float(value)
            except:
                converted[field_name] = None
        else:
            converted[field_name] = str(value) if value is not None else None

    return converted


@app.route('/', methods=['POST'])
def transform_to_avro():
    try:
        data = request.get_json()
        raw_dir = data.get('raw_dir')
        stg_dir = data.get('stg_dir')

        if not raw_dir or not stg_dir:
            return jsonify({"error": "Потрібні параметри: raw_dir та stg_dir"}), 400

        # Перевіряємо raw директорію
        if not os.path.exists(raw_dir):
            return jsonify({"error": f"Raw директорія не знайдена: {raw_dir}"}), 400

        # Очищаємо stg директорію
        if os.path.exists(stg_dir):
            shutil.rmtree(stg_dir)
        os.makedirs(stg_dir, exist_ok=True)

        # Знаходимо JSON файли
        json_files = sorted(glob.glob(os.path.join(raw_dir, '*.json')))

        if not json_files:
            return jsonify({"error": f"JSON файли не знайдені в {raw_dir}"}), 400

        total_records = 0

        # Обробляємо кожний JSON файл
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as f:
                records = json.load(f)

            if not isinstance(records, list):
                records = [records]

            # Створюємо Avro файл
            base_name = os.path.basename(json_file).replace('.json', '.avro')
            avro_file = os.path.join(stg_dir, base_name)

            with open(avro_file, 'wb') as f:
                for record in records:
                    converted = convert_record(record)
                    schemaless_writer(f, AVRO_SCHEMA, converted)
                    total_records += 1

        return jsonify({
            "status": "success",
            "records": total_records,
            "directory": stg_dir
        }), 201

    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        return jsonify({"error": error_msg}), 500


if __name__ == '__main__':
    app.run(host='localhost', port=8082, debug=False)