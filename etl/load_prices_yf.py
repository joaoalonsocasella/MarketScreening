import os
import datetime as dt
import yfinance as yf
import psycopg

print("DEBUG PG_CONN prefix:", os.environ.get("PG_CONN")[:60])

PG_CONN = os.environ["PG_CONN"]

# tickers sem sufixo
RAW_TICKERS = os.environ.get("TICKERS", "VALE3,PETR4,ITUB4,B3SA3,WEGE3").split(",")

def resolve_ticker(tk: str) -> str:
    """Testa sufixos (.SAO e .SA) e devolve o primeiro que funcionar"""
    for suffix in [".SAO", ".SA"]:
        try:
            df = yf.download(tk + suffix, period="5d", progress=False)
            if not df.empty:
                print(f"Ticker resolvido: {tk} -> {tk+suffix}")
                return tk + suffix
        except Exception as e:
            print(f"Erro ao tentar {tk+suffix}: {e}")
    print(f"Nenhum sufixo válido encontrado para {tk}, mantendo cru")
    return tk

TICKERS = [resolve_ticker(tk) for tk in RAW_TICKERS]

end = dt.date.today()
start = end - dt.timedelta(days=120)

rows_upserted = 0
with psycopg.connect(PG_CONN, autocommit=False) as conn:
    with conn.cursor() as cur:
        for tk in TICKERS:
            print(f"Baixando {tk} de {start} até {end}")
            df = yf.download(tk, start=start, end=end, progress=False).reset_index()
            if df.empty:
                print(f"Nenhum dado encontrado para {tk}, pulando...")
                continue

            df = df.rename(columns={
                'Date': 'trade_date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Adj Close': 'adj_close',
                'Volume': 'volume'
            })
            df['ticker'] = tk.replace('.SAO', '').replace('.SA', '')

            for r in df.itertuples(index=False):
                cur.execute("""
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
                """, (
                    r.trade_date.date(),
                    r.ticker,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.adj_close,
                    int(r.volume)
                ))
                rows_upserted += 1
    conn.commit()

print(f"rows_upserted={rows_upserted}")
