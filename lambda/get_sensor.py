import json

def lambda_handler(event, context):
    try:
        sensor_id = event.get('pathParameters', {}).get('sensor_id')

        return {
            "statusCode": 200,
            "body": json.dumps({"sensor_id": sensor_id})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
