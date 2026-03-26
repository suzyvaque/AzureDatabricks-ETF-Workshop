dbutils.widgets.text("usernumber", "0")
usernumber = dbutils.widgets.get("usernumber")

dbutils.widgets.text("storage_account", "adlsworkshopadb")
storage_account = dbutils.widgets.get("storage_account")

# adls_access_key = "REMOVED_SECRET" # TODO
adls_access_key = dbutils.widgets.get("adls_access_key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    adls_access_key
)

# COMMAND ----------

import sys
# %run /Workspace/Users/<your_id>/2_DeltaPipeline
workspace_root = "/Workspace/Users/suzyvauqe@stukrse.net/1_DeltaPipeline"
if workspace_root not in sys.path:
    sys.path.append(workspace_root)

# COMMAND ----------

from transformations.bronze_transformations import build_bronze
from transformations.silver_transformations import build_silver
from transformations.gold_transformations import build_gold_views

SOURCE_FMT = "delta"

build_bronze(
    spark=spark,
    usernumber=usernumber,
    source_fmt=SOURCE_FMT,
    bronze_source_value="landing"
)

build_silver(
    spark=spark,
    usernumber=usernumber
)

build_gold_views(
    spark=spark,
    usernumber=usernumber
)