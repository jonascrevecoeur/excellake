import os
import io
import json
import base64
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth
from dagster import IOManager
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaInMemoryUpload, HttpError
from google.oauth2 import service_account
import polars as pl
import pandas as pd
from dagster import OutputContext


class Excellake(IOManager):
    home_directory: str

    def __init__(self, home_directory: str):
        self.home_directory = home_directory

    def _get_path(self, context):
        path = context.asset_key.path[-1].replace("__", "/")

        return f"{self.home_directory}/{path}.xlsx"

    def handle_output(self, context: OutputContext, obj):
        filename = self._get_path(context)

        if not isinstance(obj, pl.DataFrame) and not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"The implementation of Excellake only supports pandas and polars DataFrames."
            )

        if isinstance(obj, pl.DataFrame):
            obj = obj.to_pandas()

        # Create the folder if it does not exist
        Path(filename).parent.mkdir(parents=True, exist_ok=True)

        if context.has_asset_partitions:
            with pd.ExcelWriter(
                filename,
                engine="openpyxl",
                mode="a" if os.path.exists(filename) else "w",
                if_sheet_exists="replace" if os.path.exists(filename) else None,
            ) as writer:
                obj.to_excel(
                    writer,
                    engine="openpyxl",
                    sheet_name=context.partition_key,
                    index=False,
                )

        else:
            obj.to_excel(filename, sheet_name="data")

    def load_input(self, context):
        path = self._get_path(context)

        return pl.read_excel(path, engine="openpyxl", sheet_name="data")
