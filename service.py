import json
import logging
import os
import tempfile

import httpx
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from cryptography.fernet import Fernet

paperless_token = os.environ['PAPERLESS_TOKEN']
paperless_host = os.environ['PAPERLESS_HOST']
azure_conn_str = os.environ['AZURE_CONN_STR']
document_container_name = os.environ['DOCUMENT_CONTAINER']
document_metadata_container_name = os.environ['DOCUMENT_METADATA_CONTAINER']

encryption_key = os.environ['ENCRYPTION_KEY']

blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
document_container = blob_service_client.get_container_client(document_container_name)
document_metadata_container = blob_service_client.get_container_client(document_metadata_container_name)


def encrypt_file(file_path):
    fernet = Fernet(encryption_key)
    with open(file_path, 'rb') as file:
        original = file.read()
    encrypted = fernet.encrypt(original)
    return encrypted


def upload_encrypted_file(file_path, container_client):
    encrypted_data = encrypt_file(file_path)
    blob_client = container_client.get_blob_client(file_path.split("/")[-1])
    blob_client.upload_blob(encrypted_data, standard_blob_tier=StandardBlobTier.COOL, overwrite=True)


def get_all_documents():
    headers = {"Authorization": f"Token {paperless_token}"}
    all_results = []

    try:
        with open('last_file.txt', 'r') as file:
            last_idx = int(file.read())
    except FileNotFoundError:
        last_idx = -1

    with httpx.Client() as client:
        next_url = paperless_host + "/api/documents/?ordering=-id"
        while next_url is not None:
            response = client.get(next_url, headers=headers)
            all_results += response.json()["results"]
            next_url = response.json()["next"]
            if min([result["id"] for result in all_results]) < last_idx:
                break
        all_results = list(filter(lambda x: x["id"] > last_idx, all_results))

    print(f"Retreived metadata for {len(all_results)} new documents...")

    with tempfile.TemporaryDirectory() as tmp_dir:
        for result in all_results:
            with open(f"{tmp_dir}/{result['id']}.json", "w") as f:
                json.dump(result, f, indent=1)
            upload_encrypted_file(f"{tmp_dir}/{result['id']}.json", document_metadata_container)

            print(f"Uploaded metadata for {result['id']}!")

            with httpx.Client(follow_redirects=True) as client:
                response = client.get(paperless_host + f"/api/documents/{result['id']}/download/", headers=headers)
                with open(f"{tmp_dir}/{result['id']}.pdf", 'wb') as pdf_file:
                    pdf_file.write(response.content)
                upload_encrypted_file(f"{tmp_dir}/{result['id']}.pdf", document_container)

            print(f"Uploaded document for {result['id']}!")

    if len(all_results) != 0:
        last_idx = max([result["id"] for result in all_results])
        with open("last_file.txt", "w") as f:
            f.write(str(last_idx))


if __name__ == "__main__":
    get_all_documents()
