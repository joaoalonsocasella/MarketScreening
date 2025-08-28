import os
import datetime as dt
import yfinance as yf
import psycopg2  # use psycopg2-binary no Windows

print("DEBUG PG_CONN prefix:", os.environ.get("PG_CONN")[:60])

PG_CONN = os.environ["PG_CONN"]

# lista fixa de tickers da B3 já com .SA
TICKERS = [
    "PETR4.SA",
    "VALE3.SA",
    "ITUB4.SA",
    "B3SA3.SA",
    "ABEV3.SA",
    "WEGE3.SA",
    "LREN3.SA",
    "SUZB3.SA",
    "BBAS3.SA",
]

end = dt.date.today()
start = end - dt.timedelta(days=120)

rows_upserted = 0
with psycopg2.connect(PG_CONN) as conn:
    with conn.cursor() as cur:
        for tk in TICKERS:
            print(f"Baixando {tk} de {start} até {end}")
            ticker = yf.Ticker(tk)
            df = ticker.history(start=start, end=end).reset_index()
            if df.empty:
                print(f"Nenhum dado encontrado para {tk}, pulando...")
                continue

            df = df.rename(columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            })

            # alguns ativos não têm 'Adj Close' no history -> tratar
            if "Adj Close" in df.columns:
                df = df.rename(columns={"Adj Close": "adj_close"})
            else:
                df["adj_close"] = df["close"]

            # ticker sem sufixo para salvar no banco
            df["ticker"] = tk.replace(".SA", "")

            for r in df.itertuples(index=False):
                cur.execute(
                    """
                    insert into fact_quotes_daily
                    (trade_date,ticker,open,high,low,close,adj_close,volume)
                    values (%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict (trade_date,ticker) do update set
                      open=excluded.open,
                      high=excluded.high,
                      low=excluded.low,
                      close=excluded.close,
                      adj_close=excluded.adj_close,
                      volume=excluded.volume
                """,
                    (
                        r.trade_date.date(),
                        r.ticker,
                        r.open,
                        r.high,
                        r.low,
                        r.close,
                        r.adj_close,
                        int(r.volume),
                    ),
                )
                rows_upserted += 1
    conn.commit()

print(f"rows_upserted={rows_upserted}")
