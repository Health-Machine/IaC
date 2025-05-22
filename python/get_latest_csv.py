import boto3
import json

s3 = boto3.client('s3')
BUCKET_NAME = 'raw-bucket-381492149341'

def lambda_handler(event, context):
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)

        if 'Contents' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Nenhum arquivo encontrado."})
            }

        csv_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
        if not csv_files:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Nenhum CSV encontrado."})
            }

        latest = max(csv_files, key=lambda x: x['LastModified'])

        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': latest['Key']},
            ExpiresIn=3600
        )

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"url": url})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
