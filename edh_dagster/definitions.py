from dagster import Definitions

from edh_dagster.assets import all_assets
from edh_dagster.checks import all_checks

defs = Definitions(assets=all_assets, asset_checks=all_checks)
