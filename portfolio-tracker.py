import os
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

NAME_SPACES = {"obs": "http://www.ecb.europa.eu/vocabulary/stats/exr/1"}
PATH = Path("data")
URLS = {
    "GBP": "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/gbp.xml",
    "USD": "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/usd.xml",
    "CAD": "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/cad.xml",
}
WHICH_YEAR = int(os.environ["WHICH_YEAR"])
START_DATE = date(WHICH_YEAR, 1, 1)
END_DATE = date(WHICH_YEAR + 1, 1, 1)
print("start period:", START_DATE)
print("end period:", END_DATE)


def _download_exchange_rates(currency: str) -> Path:
    res = requests.get(URLS.get(currency))
    path = PATH / f"{currency.upper()}_{str(date.today())}.xml"
    with open(path, "w") as f:
        f.write(res.text)
    return path


def _refresh_exchange_rates(currency: str, updated_at: str, path: Path) -> Path:
    if updated_at == str(date.today()):
        return path

    [path.unlink() for path in PATH.glob(f"{currency}*.xml")]
    return _download_exchange_rates(currency=currency)


def _get_exchange_rates() -> dict[str, dict[str, float]]:
    rates = {}
    for path in PATH.glob("*.xml"):
        currency, updated_at = path.name.split(".xml")[0].split("_")
        path = _refresh_exchange_rates(
            currency=currency, updated_at=updated_at, path=path
        )
        tree = ET.parse(path)
        root = tree.getroot()
        exchange_rates = root.findall(".//obs:Obs", namespaces=NAME_SPACES)
        tmp = {
            rate.get("TIME_PERIOD"): float(rate.get("OBS_VALUE"))
            for rate in exchange_rates
        }
        rates[currency] = tmp
    return rates


def _clean_miliseconds(t: str) -> str:
    return t.split(".")[0]


def _read_multiple_csvs() -> pd.DataFrame:
    transactions = pd.concat([pd.read_csv(path) for path in PATH.glob("*.csv")])
    transactions["Time"] = transactions["Time"].apply(_clean_miliseconds)
    transactions["Time"] = pd.to_datetime(transactions["Time"])
    transactions["date"] = transactions["Time"].dt.date
    transactions["date"] = transactions["date"].astype(str)
    transactions = transactions.loc[transactions["Time"] < str(END_DATE)]
    return transactions


def _get_sell_tickers(transactions: pd.DataFrame) -> list[str]:
    sell_tickers = transactions.loc[
        ((transactions.Time >= str(START_DATE)) & (transactions.Time < str(END_DATE)))
        & transactions.Action.isin(["Limit sell", "Market sell"]),
        "Ticker",
    ].unique()
    return list(sell_tickers)


def _add_exchange_rate(row: pd.Series, curr_coll: str) -> float:
    if row[curr_coll] == "EUR":
        return 1.0

    try:
        res = exchange_rates[row[curr_coll]][row["date"]]
    except KeyError as e:
        _download_exchange_rates(row[curr_coll])
        raise KeyError("Try again") from e
    return res


def _calculate_pnl(transactions: pd.DataFrame) -> float:
    needed = transactions.loc[
        ((transactions.Time >= str(START_DATE)) & (transactions.Time < str(END_DATE)))
        & transactions.Action.isin(["Limit sell", "Market sell"])
    ].copy()
    needed["rates"] = needed.apply(
        lambda row: _add_exchange_rate(row=row, curr_coll="Currency (Result)"), axis=1
    )
    needed["eur_amount"] = needed["Result"] / needed["rates"]
    return needed["eur_amount"].sum()


def _calculate_sell_amount(transactions: pd.DataFrame) -> float:
    needed = transactions.loc[
        ((transactions.Time >= str(START_DATE)) & (transactions.Time < str(END_DATE)))
        & transactions.Action.isin(["Limit sell", "Market sell"])
    ].copy()
    needed["rates"] = needed.apply(
        lambda row: _add_exchange_rate(row=row, curr_coll="Currency (Total)"), axis=1
    )
    needed["eur_amount"] = needed["Total"] / needed["rates"]
    needed.to_excel("check.xlsx", index=False)
    return needed["eur_amount"].sum()


if __name__ == "__main__":
    transactions = _read_multiple_csvs()
    exchange_rates = _get_exchange_rates()
    sell_tickers = _get_sell_tickers(transactions=transactions)
    pnl = _calculate_pnl(transactions=transactions)
    total_sell = _calculate_sell_amount(transactions=transactions)
    print("pnl:", pnl)
    print("total sell:", total_sell)
