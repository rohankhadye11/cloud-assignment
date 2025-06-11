import base64
import json
import os

from google.cloud import pubsub_v1

# Initialize Pub/Sub publisher client
# The topic ID should be provided as an environment variable in Cloud Run.
PROJECT_ID = os.environ.get('GCP_PROJECT')
PUBSUB_TOPIC_ID = os.environ.get('PUBSUB_TOPIC_ID')
PUBLISHER = pubsub_v1.PublisherClient()
TOPIC_PATH = PUBLISHER.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)

def process_gcs_file(event, context):
    """
    Cloud Run function triggered by a Cloud Storage event.
    Extracts file metadata and publishes it to a Pub/Sub topic.

    Args:
        event (dict): The Cloud Storage event payload.
        context (object): Metadata about the event.
    """
    try:
        # Log the received event for debugging
        print(f"Received Cloud Storage event: {json.dumps(event, indent=2)}")

        # Extract file information from the event payload
        file_name = event.get('name')
        file_size = event.get('size')
        file_format = event.get('contentType')
        bucket_name = event.get('bucket')

        if not all([file_name, file_size, file_format, bucket_name]):
            print("Missing one or more required file attributes (name, size, contentType, bucket). Skipping.")
            return

        # Prepare the message payload for Pub/Sub
        message_data = {
            'fileName': file_name,
            'fileSize': file_size,
            'fileFormat': file_format,
            'bucketName': bucket_name,
            'timestamp': context.timestamp # Use context for event timestamp
        }
        message_json = json.dumps(message_data)

        # Publish the message to Pub/Sub
        # Pub/Sub messages must be bytes.
        future = PUBLISHER.publish(TOPIC_PATH, message_json.encode('utf-8'))
        message_id = future.result() # Blocks until the message is published

        print(f"File '{file_name}' processed. Published message to Pub/Sub with ID: {message_id}")
        print(f"File Info: Name='{file_name}', Size='{file_size} bytes', Format='{file_format}'")

    except Exception as e:
        print(f"Error processing event: {e}")
        # Depending on your error handling strategy, you might re-raise the exception
        # to indicate a failure, which can trigger Cloud Run retries.
        # For simplicity here, we just print the error.