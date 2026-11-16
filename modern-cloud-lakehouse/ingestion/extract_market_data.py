import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

TICKERS = ["AAPL", "MSFT", "NVDA", "JPM", "GS", "AMZN", "GOOGL"]
BRONZE_BASE_DIR = Path("data/bronze")


def extract_ohlcv(tickers: list[str], period: str = "1mo", batch_id: str = "", ingested_at: str = "") -> pd.DataFrame:
    """Fetch daily OHLCV market data and append Bronze audit columns."""
    raw = yf.download(tickers, period=period, interval="1d", group_by="ticker", auto_adjust=False, progress=False, threads=False)
    records = []

    for ticker in tickers:
        df_ticker = raw[ticker].dropna(how="all").reset_index()
        df_ticker.columns = [col.lower().replace(" ", "_") for col in df_ticker.columns]
        df_ticker["ticker"] = ticker
        records.append(df_ticker)

    combined = pd.concat(records, ignore_index=True)
    combined["trade_date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined = combined.drop(columns=["date"])

    # Bronze audit metadata
    combined["_ingested_at"] = ingested_at
    combined["_source_system"] = "yahoo_finance_api"
    combined["_batch_id"] = batch_id
    return combined


def extract_ticker_metadata(tickers: list[str], batch_id: str = "", ingested_at: str = "") -> pd.DataFrame:
    """Fetch company dimension attributes for downstream SCD Type 2 tracking."""
    rows = []
    for ticker in tickers:
        info = yf.Ticker(ticker).info
        rows.append({
            "ticker": ticker,
            "company_name": info.get("longName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "exchange": info.get("exchange"),
            "currency": info.get("currency"),
            "country": info.get("country"),
            "market_cap": info.get("marketCap"),
            "_ingested_at": ingested_at,
            "_source_system": "yahoo_finance_api",
            "_batch_id": batch_id,
        })
    return pd.DataFrame(rows)


def write_bronze_parquet(df: pd.DataFrame, table_name: str, ingest_date: str) -> Path:
    """Write DataFrame as partitioned Parquet in the Bronze layer."""
    output_dir = BRONZE_BASE_DIR / table_name / f"ingest_date={ingest_date}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{table_name}_{uuid.uuid4().hex[:8]}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_file, compression="snappy")
    return output_file


def main() -> None:
    now_utc = datetime.now(timezone.utc)
    ingest_date = now_utc.strftime("%Y-%m-%d")
    ingested_at = now_utc.isoformat()
    batch_id = str(uuid.uuid4())

    print(f"[{ingested_at}] Starting Bronze ingestion | batch_id={batch_id}")

    ohlcv_df = extract_ohlcv(TICKERS, period="1mo", batch_id=batch_id, ingested_at=ingested_at)
    ohlcv_path = write_bronze_parquet(ohlcv_df, "market_ohlcv", ingest_date)
    print(f"Wrote {len(ohlcv_df)} OHLCV rows -> {ohlcv_path}")

    meta_df = extract_ticker_metadata(TICKERS, batch_id=batch_id, ingested_at=ingested_at)
    meta_path = write_bronze_parquet(meta_df, "ticker_metadata", ingest_date)
    print(f"Wrote {len(meta_df)} metadata rows -> {meta_path}")


if __name__ == "__main__":
    main()
