from dagster import Definitions, load_assets_from_modules
from .resources import RESOURCES_DEV

from . import assets
from . import jobs

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources=RESOURCES_DEV,
    jobs=[
        jobs.check_connections,
    ],
)
