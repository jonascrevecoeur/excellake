import os
from dotenv import load_dotenv

from dagster import Definitions, load_assets_from_package_module, define_asset_job
from excellake.client.excellake import Excellake
import excellake.assets

from excellake.jobs.daily_job import daily_job

load_dotenv()


all_assets_job = define_asset_job(name="all_assets_job")


defs = Definitions(
    assets=load_assets_from_package_module(excellake.assets),
    resources={"excellake": Excellake(home_directory=os.environ.get("HOME_DIRECTORY"))},
    jobs=[all_assets_job, daily_job],
)
