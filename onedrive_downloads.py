import os
import time
import requests
from msal import ConfidentialClientApplication
from http.client import IncompleteRead

# Azure AD application details (replace with your details)
CLIENT_ID = 'your-client-id'
CLIENT_SECRET = 'your-client-secret'
TENANT_ID = 'your-tenant-id'
AUTHORITY_URL = f'https://login.microsoftonline.com/{TENANT_ID}'
GRAPH_API_URL = 'https://graph.microsoft.com/v1.0'
USER_EMAIL = 'user@example.com'  # Replace with the user's email address

# Initialize MSAL client
app = ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY_URL, client_credential=CLIENT_SECRET)

# Get access token
def get_access_token():
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(f"Could not obtain access token: {result.get('error_description')}")

# Download files and folders recursively
def download_files(folder_url, local_path, access_token):
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(folder_url, headers=headers).json()

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    if 'value' in response:
        for item in response['value']:
            item_name = item['name']
            if item.get('folder'):
                # If it's a folder, recurse into it
                folder_children_url = f"{GRAPH_API_URL}/users/{USER_EMAIL}/drive/items/{item['id']}/children"
                download_files(folder_children_url, os.path.join(local_path, item_name), access_token)
            elif '@microsoft.graph.downloadUrl' in item:
                # If it's a file, download it
                download_url = item['@microsoft.graph.downloadUrl']
                local_file_path = os.path.join(local_path, item_name)
                download_file_with_resume(download_url, local_file_path)

# Download file in chunks with retry logic and resume support
def download_file_with_resume(url, local_file_path, retries=5, chunk_size=65536, timeout=30):
    attempt = 0
    backoff = 1

    while attempt < retries:
        try:
            headers = {}
            file_size = 0

            # Check if the file exists and get its size to resume
            if os.path.exists(local_file_path):
                file_size = os.path.getsize(local_file_path)
                headers['Range'] = f'bytes={file_size}-'  # Set the range header to resume the download

            with requests.get(url, headers=headers, stream=True, timeout=timeout) as response:
                response.raise_for_status()  # Ensure the request was successful
                mode = 'ab' if file_size > 0 else 'wb'  # Append if resuming, write if starting new
                with open(local_file_path, mode) as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
            print(f"Downloaded: {local_file_path}")
            return  # Exit after successful download
        except (requests.exceptions.ConnectionError, IncompleteRead) as e:
            attempt += 1
            print(f"Error: {e}, retrying in {backoff} seconds... ({attempt}/{retries})")
            time.sleep(backoff)  # Exponential backoff between retries
            backoff *= 2  # Double the wait time for the next retry

    print(f"Failed to download {local_file_path} after {retries} attempts")

def main():
    try:
        # Obtain access token
        access_token = get_access_token()

        # Get the user's OneDrive root folder and download its contents
        drive_url = f"{GRAPH_API_URL}/users/{USER_EMAIL}/drive/root/children"
        local_download_path = './OneDrive_Download'

        download_files(drive_url, local_download_path, access_token)
        print(f"Download completed. Files are saved to: {local_download_path}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
