import json
import os
import sys # Import sys for flushing stdout and stderr
import traceback # Import traceback for printing full stack traces

from google.cloud import pubsub_v1

# Initialize Pub/Sub publisher client
# The topic ID should be provided as an environment variable in Cloud Run.
PROJECT_ID = os.environ.get('GCP_PROJECT')
PUBSUB_TOPIC_ID = os.environ.get('PUBSUB_TOPIC_ID')

# Check if required environment variables are set before proceeding
if not PROJECT_ID:
    print("Error: GCP_PROJECT environment variable not set.", file=sys.stderr, flush=True)
    # In a real application, you might raise an exception or handle this more gracefully.
    # For debugging, exiting is fine to highlight the issue.
    # sys.exit(1) # Commented out to allow function to attempt execution for further debugging

if not PUBSUB_TOPIC_ID:
    print("Error: PUBSUB_TOPIC_ID environment variable not set. Please set it in Cloud Run environment variables.", file=sys.stderr, flush=True)
    # sys.exit(1) # Commented out for similar reasons as above

# Initialize PublisherClient and Topic Path only if environment variables are available
# This prevents errors during initialization if env vars are missing, allowing debug logs to show.
PUBLISHER = None
TOPIC_PATH = None
if PROJECT_ID and PUBSUB_TOPIC_ID:
    try:
        PUBLISHER = pubsub_v1.PublisherClient()
        TOPIC_PATH = PUBLISHER.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)
        print(f"Pub/Sub publisher initialized for topic: {TOPIC_PATH}", flush=True)
    except Exception as e:
        print(f"Error initializing Pub/Sub publisher: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)


def process_gcs_file(event, context=None):
    """
    Cloud Run function triggered by a Cloud Storage event.
    Extracts file metadata and publishes it to a Pub/Sub topic.

    Args:
        event (dict): The Cloud Storage event payload.
        context (object, optional): Metadata about the event.
    """
    # --- START DEBUGGING LOGS (Very early to ensure they always show) ---
    print("--- Function process_gcs_file invoked ---", flush=True)
    print(f"Raw Event Payload Type: {type(event)}", flush=True)
    print(f"Raw Event Payload: {json.dumps(event, indent=2)}", flush=True)
    print(f"Raw Context Object Type: {type(context)}", flush=True)
    print(f"Raw Context Object: {context}", flush=True) # Context might not be JSON serializable directly
    # --- END DEBUGGING LOGS ---

    try:
        # Extract timestamp and other context details safely
        timestamp = 'N/A (Context not provided or attribute missing)'
        event_id = 'N/A (Context not provided or attribute missing)'
        event_type = 'N/A (Context not provided or attribute missing)'
        resource = 'N/A (Context not provided or attribute missing)'

        if context:
            print(f"Context attributes: {[attr for attr in dir(context) if not attr.startswith('_')]}", flush=True)
            timestamp = getattr(context, 'timestamp', timestamp)
            event_id = getattr(context, 'event_id', event_id)
            event_type = getattr(context, 'event_type', event_type)
            resource = getattr(context, 'resource', resource)
            print(f"Context details: ID={event_id}, Type={event_type}, Timestamp={timestamp}, Resource={resource}", flush=True)
        else:
            print("Context object is None.", flush=True)


        # Extract file information from the event payload
        # Cloud Storage events from Eventarc for 'object.v1.finalized' usually have
        # the relevant data fields directly at the top level of the 'event' dict.
        # However, some older setups or frameworks might wrap it under a 'data' key.
        file_name = event.get('name')
        file_size = event.get('size')
        file_format = event.get('contentType')
        bucket_name = event.get('bucket')

        # If direct access fails, check if data is nested under 'data' key
        if not all([file_name, file_size, file_format, bucket_name]):
            print("Direct event attributes not found. Checking for nested 'data' field...", flush=True)
            event_data = event.get('data')
            if event_data and isinstance(event_data, dict):
                print("Found 'data' field. Attempting to extract from there.", flush=True)
                file_name = event_data.get('name')
                file_size = event_data.get('size')
                file_format = event_data.get('contentType')
                bucket_name = event_data.get('bucket')
            else:
                print("No 'data' field found or it's not a dictionary.", flush=True)

        if not all([file_name, file_size, file_format, bucket_name]):
            print(f"Critical: Missing one or more required file attributes (name, size, contentType, bucket) even after checking 'data' field. Event: {json.dumps(event, indent=2)}", file=sys.stderr, flush=True)
            return

        # Prepare the message payload for Pub/Sub
        message_data = {
            'fileName': file_name,
            'fileSize': file_size,
            'fileFormat': file_format,
            'bucketName': bucket_name,
            'timestamp': timestamp # Use the timestamp extracted from context or 'N/A'
        }
        message_json = json.dumps(message_data)

        # Ensure Pub/Sub publisher is initialized before attempting to publish
        if PUBLISHER and TOPIC_PATH:
            # Publish the message to Pub/Sub
            # Pub/Sub messages must be bytes.
            future = PUBLISHER.publish(TOPIC_PATH, message_json.encode('utf-8'))
            message_id = future.result() # Blocks until the message is published

            print(f"File '{file_name}' processed. Published message to Pub/Sub with ID: {message_id}", flush=True)
            print(f"File Info: Name='{file_name}', Size='{file_size} bytes', Format='{file_format}'", flush=True)
        else:
            print("Pub/Sub publisher not initialized. Cannot publish message.", file=sys.stderr, flush=True)

    except Exception as e:
        print(f"Unhandled Error during event processing: {e}", file=sys.stderr, flush=True)
        # Print full traceback for detailed debugging
        traceback.print_exc(file=sys.stderr)

