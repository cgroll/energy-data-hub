from edh_dagster.assets.redispatch import redispatch_assets
from edh_dagster.assets.smard import smard_assets

all_assets = [*smard_assets, *redispatch_assets]
