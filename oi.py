from datetime import datetime
import requests

from tinydb import TinyDB

oi_url = {
    "nifty": "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
    "banknifty": "https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY",
}
HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, "
    "like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    "accept-language": "en,gu;q=0.9,hi;q=0.8",
    "accept-encoding": "gzip, deflate, br",
}


def get_oi(instrument):
    url = oi_url[f"{instrument}"]
    response = requests.get(url=url, headers=HEADERS, timeout=5)

    res = response.json()
    filtered = res["filtered"]
    records = res["records"]
    total_oi_ce = filtered["CE"]["totOI"]
    total_oi_pe = filtered["PE"]["totOI"]
    underlying = records["underlyingValue"]
    total_pcr = total_oi_pe / total_oi_ce
    print(f"TOTAL PCR = {total_pcr}")

    expiry = records["expiryDates"][0]
    weekly_put_oi, weekly_call_oi = 0, 0
    for d in filtered["data"]:
        if d["expiryDate"] == expiry and abs(d["strikePrice"] - int(underlying)) < 1000:
            print(f"{expiry=}")
            print("strikeprice=", d["strikePrice"])
            weekly_put_oi += d["PE"]["changeinOpenInterest"]
            weekly_call_oi += d["CE"]["changeinOpenInterest"]
            print("-------------")
    pcr = weekly_put_oi / weekly_call_oi
    return {
        "weekly_ce_oi": weekly_call_oi,
        "weekly_pe_oi": weekly_put_oi,
        "weekly_pcr": float("{:.2f}".format(pcr)),
        "total_pcr": float("{:.2f}".format(total_pcr)),
        "underlying": underlying,
        "time": datetime.now().time().isoformat(),
    }


def save_to_db(instru):
    today = datetime.now().date().isoformat()
    db = TinyDB(
        f"/mnt/c/Users/prasa/Desktop/oi-change-plot/db/{instru}/oi_{today}.json"
    )

    oi_data = get_oi(instru)
    db.insert(oi_data)


if __name__ == "__main__":
    import sys

    instru = sys.argv[1]
    save_to_db(instru)