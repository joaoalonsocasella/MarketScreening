import os, datetime as dt
import yfinance as yf
import psycopg

PG_CONN = os.environ["PG_CONN"]
TICKERS = os.environ.get("TICKERS", "VALE3.SA,PETR4.SA,ITUB4.SA,B3SA3.SA,WEGE3.SA").split(",")

end = dt.date.today()
start = end - dt.timedelta(days=120)

rows_upserted = 0
with psycopg.connect(PG_CONN, autocommit=False) as conn:
    with conn.cursor() as cur:
        for tk in TICKERS:
            df = yf.download(tk, start=start, end=end).reset_index()
            df = df.rename(columns={
                'Date':'trade_date','Open':'open','High':'high','Low':'low',
                'Close':'close','Adj Close':'adj_close','Volume':'volume'
            })
            df['ticker'] = tk.replace('.SA','')
            for r in df.itertuples(index=False):
                cur.execute("""
                    insert into fact_quotes_daily
                    (trade_date,ticker,open,high,low,close,adj_close,volume)
                    values (%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict (trade_date,ticker) do update set
                      open=excluded.open, high=excluded.high, low=excluded.low,
                      close=excluded.close, adj_close=excluded.adj_close, volume=excluded.volume
                """, (r.trade_date.date(), r.ticker, r.open, r.high, r.low, r.close, r.adj_close, int(r.volume)))
                rows_upserted += 1
    conn.commit()

print(f"rows_upserted={rows_upserted}")
