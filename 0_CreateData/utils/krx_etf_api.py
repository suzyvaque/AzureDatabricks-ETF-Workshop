import requests
import pandas as pd


def call_krx_etf_by_date(
    bas_dd: str,
    auth_key: str,
) -> pd.DataFrame:
    """
    Call KRX ETF daily API.

    bas_dd: YYYYMMDD
    """

    url = "https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd"

    params = {
        "basDd": bas_dd
    }

    headers = {
        "AUTH_KEY": auth_key
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()
    df = pd.json_normalize(data["OutBlock_1"])
    return df
