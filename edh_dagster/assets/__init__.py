from edh_dagster.assets.capacity import capacity_assets
from edh_dagster.assets.gas import gas_assets
from edh_dagster.assets.mastr import mastr_assets
from edh_dagster.assets.pecd import pecd_assets
from edh_dagster.assets.redispatch import redispatch_assets
from edh_dagster.assets.smard import smard_assets

all_assets = [*smard_assets, *redispatch_assets, *mastr_assets, *pecd_assets, *capacity_assets, *gas_assets]
