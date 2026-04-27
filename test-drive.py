import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io
import os
import pandas as pd
import datetime

# 1. Authenticate using WIF (Application Default Credentials)
# The 'scopes' define what the script is allowed to do.
SCOPES = ['https://www.googleapis.com/auth/drive']
creds, project = google.auth.default(scopes=SCOPES)

service = build('drive', 'v3', credentials=creds)

# 2. Configuration
# Replace this with your actual Folder ID from the Drive URL
FOLDER_ID = os.getenv('DRIVE_DIR_ID')

def upload_to_st(filename, content_string):
    """Creates or updates a file in the /st/ folder."""
    file_metadata = {
        'name': filename,
        'parents': [FOLDER_ID]
    }
    
    # Create a dummy file locally to upload
    with open(filename, "w") as f:
        f.write(content_string)
        
    media = MediaFileUpload(filename, mimetype='text/plain', resumable=True)
    
    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    print(f"Uploaded successfully. File ID: {file.get('id')}")
    os.remove(filename) # Clean up local runner storage

def read_from_st(filename):
    """Searches for a file in /st/ and reads its content."""
    query = f"name = '{filename}' and '{FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print(f"No file named {filename} found in folder.")
        return None

    file_id = items[0]['id']
    request = service.files().get_media(fileId=file_id)
    
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    fh.seek(0) 
    df = pd.read_csv(fh)
    return df

# --- TEST EXECUTION ---
if __name__ == "__main__":
    test_filename = "cache.csv"
    test_content = "Hello from GitHub Actions via WIF!"

    # print(f"Testing upload to folder {FOLDER_ID}...")
    # upload_to_st(test_filename, test_content)

    print(f"Testing read from folder {FOLDER_ID}...")
    df = read_from_st(test_filename)
    print(f"Retrieved Content")
    
    print(f"Upload csv to folder {FOLDER_ID}...")
    output_filename = f"output-{datetime.date.today().isoformat()}.csv"
    df.to_csv(output_filename, index=False)
    file_metadata = {
        'name': output_filename
    }
    media = MediaFileUpload(output_filename, resumable=True)
    updated_file = service.files().update(
        fileId=os.getenv('OUTPUT_FILE_ID'),
        body=file_metadata,
        media_body=media,
        fields='id, name'
    ).execute()
    os.remove(output_filename)
    print(f'updated to {output_filename}')
