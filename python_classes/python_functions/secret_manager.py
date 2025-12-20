from google.cloud import secretmanager
import google_crc32c
import json

def get_values_in_secrets(project_id, secret_id, version_id="latest"):
    # Create the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build secret version name
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)

    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise ValueError("Data corruption detected")

    # Decode secret payload
    payload = response.payload.data.decode("UTF-8") 
    return payload
# Example usage
project_id = "175579495168"    
secret_ids = ["SENDER_EMAIL", "SENDER_PASSWORD"]

# hey i need to print the values stored in two secrets like SENDER_EMAIL and 
# SENDER_PASSWORD 
for secret_id in secret_ids:
    result = get_values_in_secrets(project_id, secret_id)
    print(result)
