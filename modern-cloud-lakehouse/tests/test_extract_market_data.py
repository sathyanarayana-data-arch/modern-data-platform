import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

# Ensure ingestion/ is importable regardless of working directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))

import extract_market_data as emd  # noqa: E402


@pytest.fixture
def mock_ohlcv_download() -> pd.DataFrame:
    """Simulated multi-index DataFrame returned by yf.download(..., group_by='ticker')."""
    dates = pd.to_datetime(["2026-11-13", "2026-11-16"])
    tickers = ["AAPL", "MSFT"]
    metrics = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

    columns = pd.MultiIndex.from_product([tickers, metrics], names=["Ticker", "Price"])
    data = [
        [225.0, 228.5, 224.1, 227.8, 227.8, 45_000_000, 415.0, 420.0, 414.2, 419.5, 419.5, 22_000_000],
        [228.0, 230.2, 227.5, 229.9, 229.9, 41_200_000, 420.1, 423.4, 419.0, 422.8, 422.8, 19_800_000],
    ]
    df = pd.DataFrame(data, index=dates, columns=columns)
    df.index.name = "Date"
    return df


def test_extract_ohlcv_schema_and_audit_columns(mock_ohlcv_download: pd.DataFrame) -> None:
    """Validate normalized snake_case schema, row count, and Bronze audit columns."""
    batch_id = "test-batch-1234"
    ingested_at = "2026-11-16T12:00:00+00:00"

    with patch("extract_market_data.yf.download", return_value=mock_ohlcv_download) as mock_dl:
        df = emd.extract_ohlcv(
            tickers=["AAPL", "MSFT"],
            ingested_at=ingested_at,
            batch_id=batch_id,
            period="5d",
        )

    mock_dl.assert_called_once()
    expected_cols = {
        "ticker",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "_ingested_at",
        "_source_system",
        "_batch_id",
    }
    assert expected_cols.issubset(set(df.columns))
    assert len(df) == 4  # 2 tickers x 2 dates
    assert set(df["ticker"]) == {"AAPL", "MSFT"}
    assert (df["_batch_id"] == batch_id).all()
    assert (df["_ingested_at"] == ingested_at).all()
    assert (df["_source_system"] == "yahoo_finance_api").all()
    assert df["trade_date"].tolist() == ["2026-11-13", "2026-11-16", "2026-11-13", "2026-11-16"]


def test_extract_ticker_metadata_schema_and_audit_columns() -> None:
    """Validate dimension attribute extraction and audit metadata stamping."""
    batch_id = "test-batch-5678"
    ingested_at = "2026-11-16T12:00:00+00:00"

    fake_ticker = MagicMock()
    fake_ticker.info = {
        "longName": "Apple Inc.", "shortName": "Apple Inc.",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "exchange": "NMS",
        "currency": "USD",
        "country": "United States",
        "marketCap": 3_450_000_000_000,
    }

    with patch("extract_market_data.yf.Ticker", return_value=fake_ticker):
        df = emd.extract_ticker_metadata(
            tickers=["AAPL"],
            ingested_at=ingested_at,
            batch_id=batch_id,
        )

    expected_cols = {
        "ticker",
        "company_name",
        "sector",
        "industry",
        "exchange",
        "currency",
        "country",
        "market_cap",
        "_ingested_at",
        "_source_system",
        "_batch_id",
    }
    assert expected_cols.issubset(set(df.columns))
    assert len(df) == 1
    assert df.loc[0, "ticker"] == "AAPL"
    assert df.loc[0, "company_name"] == "Apple Inc."
    assert df.loc[0, "_batch_id"] == batch_id
    assert df.loc[0, "_source_system"] == "yahoo_finance_api"


def test_write_bronze_parquet_partitioning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Validate Hive-style ingest_date=YYYY-MM-DD partitioning and Snappy Parquet readability."""
    monkeypatch.setattr(emd, "BRONZE_BASE_DIR", tmp_path)

    sample_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "close": [229.9, 422.8],
            "_ingested_at": ["2026-11-16T12:00:00+00:00"] * 2,
            "_source_system": ["yahoo_finance_api"] * 2,
            "_batch_id": ["batch-001"] * 2,
        }
    )

    output_path = emd.write_bronze_parquet(
        df=sample_df,
        table_name="market_ohlcv",
        ingest_date="2026-11-16",
    )

    assert output_path.exists()
    assert output_path.parent == tmp_path / "market_ohlcv" / "ingest_date=2026-11-16"
    assert output_path.name.startswith("market_ohlcv_")
    assert output_path.suffix == ".parquet"

    # Verify Parquet metadata & round-trip data integrity
    parquet_file = pq.ParquetFile(output_path)
    assert parquet_file.metadata.row_group(0).column(0).compression == "SNAPPY"

    read_back = pq.read_table(output_path).to_pandas()
    pd.testing.assert_frame_equal(read_back, sample_df)
