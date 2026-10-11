"""Column mappings shared by the Spark and pandas Silver engines (no heavy imports here)."""

UDIFF = {"TradDt": "trade_date", "TckrSymb": "symbol", "ISIN": "isin", "SctySrs": "series",
         "FinInstrmNm": "company_name", "OpnPric": "open", "HghPric": "high", "LwPric": "low",
         "ClsPric": "close", "PrvsClsgPric": "prev_close", "TtlTradgVol": "volume", "TtlTrfVal": "turnover",
         "TtlNbOfTxsExctd": "trades"}
LEGACY = {"TIMESTAMP": "trade_date", "SYMBOL": "symbol", "ISIN": "isin", "SERIES": "series",
          "OPEN": "open", "HIGH": "high", "LOW": "low", "CLOSE": "close", "PREVCLOSE": "prev_close",
          "TOTTRDQTY": "volume", "TOTTRDVAL": "turnover", "TOTALTRADES": "trades"}
COLUMNS = ["trade_date", "symbol", "isin", "series", "company_name", "open", "high", "low", "close",
           "prev_close", "volume", "turnover", "trades", "deliv_qty", "deliv_per"]
