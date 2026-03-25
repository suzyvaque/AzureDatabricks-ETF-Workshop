from dataclasses import dataclass


@dataclass(frozen=True)
class Env:
    CATALOG: str = "cat_adb_workshop"

    LANDING_BASE: str = "abfss://landing@adlsworkshopadb.dfs.core.windows.net/data"


TABLES = [
    "customer_cash_portfolio",
    "customer_etf_portfolio",
    "customer_master",
    "etf_master",
    "etf_trades",
]


LANDING_TABLE_FOLDERS = {
    "customer_cash_portfolio": "customer_cash_portfolio",
    "customer_etf_portfolio": "customer_etf_portfolio",
    "customer_master": "customer_master",
    "etf_master": "etf_master",
    "etf_trades": "etf_trades",
}


PRIMARY_KEYS = {
    "customer_cash_portfolio": ["customer_id", "bas_dd"],
    "customer_etf_portfolio": ["customer_id", "bas_dd", "etf_code"],
    "customer_master": ["customer_id", "bas_dd"],
    "etf_master": ["isu_cd", "bas_dd"],
    "etf_trades": ["isu_cd", "bas_dd"],
}


def bronze_schema_name(usernumber: str) -> str:
    return f"sch_user{usernumber}_bronze"


def silver_schema_name(usernumber: str) -> str:
    return f"sch_user{usernumber}_silver"


def gold_schema_name(usernumber: str) -> str:
    return f"sch_user{usernumber}_gold"


def bronze_table_name(usernumber: str, table_name: str) -> str:
    return f"{Env.CATALOG}.{bronze_schema_name(usernumber)}.{table_name}"


def silver_table_name(usernumber: str, table_name: str) -> str:
    return f"{Env.CATALOG}.{silver_schema_name(usernumber)}.{table_name}"


def gold_view_name(usernumber: str, view_name: str) -> str:
    return f"{Env.CATALOG}.{gold_schema_name(usernumber)}.{view_name}"


def landing_path(table_name: str) -> str:
    folder = LANDING_TABLE_FOLDERS.get(table_name)
    if not folder:
        raise ValueError(f"Unknown table_name: {table_name}")
    return f"{Env.LANDING_BASE.rstrip('/')}/{folder}"
