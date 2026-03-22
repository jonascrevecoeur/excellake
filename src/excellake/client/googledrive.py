import os
import io
import json
import base64

import requests
from requests.auth import HTTPBasicAuth
from dagster import IOManager
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaInMemoryUpload, HttpError
from google.oauth2 import service_account
import polars as pl
import pandas as pd


class GoogleDrive(IOManager):
    token: str
    folder_id: str
    session: requests.Session
    client: Resource

    def __init__(self, token: str, folder_id: str):
        self.folder_id = folder_id

        service_account_info = json.loads(base64.b64decode(token).decode("utf-8"))

        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=["https://www.googleapis.com/auth/drive"]
        )

        self.client = build("drive", "v3", credentials=credentials)

    def _get_or_create_folder(self, folder_name) -> str:
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='{folder_name}' "
            f"and '{self.folder_id}' in parents "
            f"and trashed=false"
        )

        results = (
            self.client.files()
            .list(q=query, fields="files(id, name)", spaces="drive")
            .execute()
        )

        if results["files"]:
            return results["files"][0]["id"]

        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [self.folder_id],
        }

        folder = self.client.files().create(body=folder_metadata, fields="id").execute()

        return folder["id"]

    def _get_file_location(self, context) -> tuple[str, str]:
        file_extension = "xls" if context.metadata.get("is_excel", False) else "csv"
        if context.has_partition_key:
            return (
                self._get_or_create_folder("_".join(context.asset_key.path)),
                f"{context.partition_key}.{file_extension}",
            )
        else:
            return self.folder_id, (
                "_".join(context.asset_key.path) + f".{file_extension}"
            )

    def _get_path(self, context):
        asset_key = context.asset_key.path[-1]
        folder = context.metadata.get("folder")

        if not folder:
            raise KeyError(
                f"Asset {asset_key} of type OneDrive misses required parameter folder."
            )

        return f"{self.base_url}/{asset_key}.xls"

    def handle_output(self, context, obj):
        folder, filename = self._get_file_location(context)

        if not isinstance(obj, pl.DataFrame) and not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"The implementation of Onedrive only supports pandas and polars DataFrames."
            )

        if isinstance(obj, pl.DataFrame):
            obj = obj.to_pandas()

        buffer = io.StringIO()
        obj.to_csv(buffer, index=False)
        bytes = buffer.getvalue().encode("utf-8")

        try:
            (
                self.client.files()
                .create(
                    body={
                        "name": filename,
                        "parents": [folder],
                    },
                    media_body=MediaInMemoryUpload(
                        bytes,
                        mimetype="text/plain",
                    ),
                    fields="id",
                    supportsAllDrives=True,
                )
                .execute()
            )
        except HttpError as e:
            context.log.error(f"Upload failed: {e}")
            raise

    def load_input(self, context):
        path = self._get_path(context)

        response = self.session.get(path)

        if response.status_code == 200:
            return pl.DataFrame({"test": [0, 1, 2]})
        else:
            print(
                f"Failed to download asset {context.asset_key.path[-1]}: {response.status_code} {response.text}"
            )
