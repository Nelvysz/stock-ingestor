GLOBAL_SCAN_API_URL = "https://scanner.tradingview.com/global/scan"
GLOBAL_SCAN_API_BODY = {
    "filter": [
         {
            "left": "is_primary",
            "operation": "equal",
            "right": True
        }
    ],
    "options": {
        "lang": "en"
    },
    "markets": ["america", "uk", "india", "spain", "russia", "australia", "brazil", "japan", "newzealand", "turkey", "switzerland", "hongkong", "taiwan", "netherlands", "belgium", "portugal", "france", "mexico", "canada", "colombia", "uae", "nigeria", "singapore", "germany", "pakistan", "peru", "poland", "italy", "argentina", "israel", "egypt", "srilanka", "serbia", "chile", "china", "malaysia", "morocco", "ksa", "bahrain", "qatar", "indonesia", "finland", "iceland", "denmark", "romania", "hungary", "sweden", "slovakia", "lithuania", "luxembourg", "estonia", "latvia", "vietnam", "rsa", "thailand", "tunisia", "korea", "kenya", "kuwait", "norway", "philippines", "greece", "venezuela", "cyprus", "bangladesh", "austria", "czech"
                ],
    "symbols": {
        "query": {
            "types": []
        },
        "tickers": []
    },
    "columns": [
        "name",
        "description",
        "exchange"
    ],
    "sort": {
        "sortBy": "market_cap_basic",
        "sortOrder": "desc"
    }
}

YAHOO_FINANCE_API_URL = "https://query1.finance.yahoo.com/v1/finance/search"

DB_HOST = "xxxxx"
DB_USERNAME = "xxxxx"
DB_PASSWORD = "xxxxx"
DB_NAME = "xxxxx"
DB_PORT = "xxxxx"
DB_INDICATOR_TABLE = "xxxxx"
DB_GLOBAL_PRICE_TABLE = "xxxxx"

