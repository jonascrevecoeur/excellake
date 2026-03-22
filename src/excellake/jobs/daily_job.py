from dagster import (
    AssetSelection,
    load_assets_from_package_module,
    define_asset_job,
    DailyPartitionsDefinition,
)

import excellake.assets

all_assets = load_assets_from_package_module(excellake.assets)


def daily_or_unpartitioned_selection(all_assets):
    selected_keys = []

    for asset_def in all_assets:
        partitions_def = asset_def.partitions_def

        if partitions_def is None:
            selected_keys.append(asset_def.key)
        elif isinstance(partitions_def, DailyPartitionsDefinition):
            selected_keys.append(asset_def.key)

    return AssetSelection.keys(*selected_keys)


daily_job = define_asset_job(
    name="daily_job",
    selection=daily_or_unpartitioned_selection(all_assets),
)
