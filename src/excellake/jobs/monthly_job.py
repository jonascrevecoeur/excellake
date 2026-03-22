from dagster import (
    AssetSelection,
    load_assets_from_package_module,
    define_asset_job,
    MonthlyPartitionsDefinition,
)

import excellake.assets

all_assets = load_assets_from_package_module(excellake.assets)


def monthly_selection(all_assets):
    selected_keys = []

    for asset_def in all_assets:
        partitions_def = asset_def.partitions_def

        if isinstance(partitions_def, MonthlyPartitionsDefinition):
            selected_keys.append(asset_def.key)

    return AssetSelection.keys(*selected_keys)


monthly_job = define_asset_job(
    name="monthly_job",
    selection=monthly_selection(all_assets),
)
