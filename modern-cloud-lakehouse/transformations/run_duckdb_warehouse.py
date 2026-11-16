"""Local DuckDB Warehouse Runner for Silver & Gold Medallion Layers.

Reads raw Hive-partitioned Parquet files from data/bronze/, applies
incremental deduplication and SCD Type 2 dimension tracking in the silver
schema, materializes the Gold Star Schema in the gold schema, and enforces
automated data quality assertions.
"""

from pathlib import Path
import duckdb

DB_PATH = Path("data/warehouse/lakehouse.duckdb")
OHLCV_GLOB = "data/bronze/market_ohlcv/**/*.parquet"
META_GLOB = "data/bronze/ticker_metadata/**/*.parquet"


def build_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    """Create schemas and execute Bronze -> Silver -> Gold transformations."""
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # 1. Silver Incremental / Idempotent Merge: silver.silver_market_ohlcv
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS silver.silver_market_ohlcv (
            trade_id VARCHAR PRIMARY KEY,
            ticker VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE NOT NULL,
            adj_close_price DOUBLE,
            trading_volume BIGINT NOT NULL,
            daily_price_change DOUBLE,
            daily_return_pct DOUBLE,
            _ingested_at TIMESTAMP,
            _source_system VARCHAR,
            _batch_id VARCHAR,
            _silver_updated_at TIMESTAMP
        );

        INSERT OR REPLACE INTO silver.silver_market_ohlcv
        WITH raw_ohlcv AS (
            SELECT
                md5(concat(trim(upper(ticker)), '|', cast(trade_date AS VARCHAR))) AS trade_id,
                trim(upper(ticker)) AS ticker,
                cast(trade_date AS DATE) AS trade_date,
                cast(open AS DOUBLE) AS open_price,
                cast(high AS DOUBLE) AS high_price,
                cast(low AS DOUBLE) AS low_price,
                cast(close AS DOUBLE) AS close_price,
                cast(adj_close AS DOUBLE) AS adj_close_price,
                cast(volume AS BIGINT) AS trading_volume,
                cast(_ingested_at AS TIMESTAMP) AS _ingested_at,
                _source_system,
                _batch_id
            FROM read_parquet('{OHLCV_GLOB}', hive_partitioning=true)
            WHERE ticker IS NOT NULL
              AND trade_date IS NOT NULL
              AND close IS NOT NULL
              AND close > 0
              AND volume >= 0
        ),
        deduplicated AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY ticker, trade_date
                    ORDER BY _ingested_at DESC
                ) AS rn
            FROM raw_ohlcv
        )
        SELECT
            trade_id,
            ticker,
            trade_date,
            round(open_price, 4) AS open_price,
            round(high_price, 4) AS high_price,
            round(low_price, 4) AS low_price,
            round(close_price, 4) AS close_price,
            round(adj_close_price, 4) AS adj_close_price,
            trading_volume,
            round(close_price - open_price, 4) AS daily_price_change,
            round(((close_price - open_price) / nullif(open_price, 0)) * 100, 4) AS daily_return_pct,
            _ingested_at,
            _source_system,
            _batch_id,
            current_timestamp AS _silver_updated_at
        FROM deduplicated
        WHERE rn = 1;
        """
    )

    # 2. Silver SCD Type 2 Snapshot: silver.snap_ticker_metadata
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS silver.snap_ticker_metadata (
            dbt_scd_id VARCHAR PRIMARY KEY,
            ticker VARCHAR NOT NULL,
            company_name VARCHAR,
            sector VARCHAR,
            industry VARCHAR,
            exchange VARCHAR,
            currency VARCHAR,
            country VARCHAR,
            market_cap BIGINT,
            _ingested_at TIMESTAMP,
            _source_system VARCHAR,
            _batch_id VARCHAR,
            dbt_valid_from TIMESTAMP NOT NULL,
            dbt_valid_to TIMESTAMP
        );

        CREATE OR REPLACE TEMP TABLE stg_ticker_metadata AS
        WITH ranked_metadata AS (
            SELECT
                trim(upper(ticker)) AS ticker,
                coalesce(nullif(trim(company_name), ''), trim(upper(ticker))) AS company_name,
                coalesce(nullif(trim(sector), ''), 'Unknown') AS sector,
                coalesce(nullif(trim(industry), ''), 'Unknown') AS industry,
                coalesce(nullif(trim(exchange), ''), 'Unknown') AS exchange,
                coalesce(nullif(trim(currency), ''), 'USD') AS currency,
                coalesce(nullif(trim(country), ''), 'Unknown') AS country,
                cast(market_cap AS BIGINT) AS market_cap,
                cast(_ingested_at AS TIMESTAMP) AS _ingested_at,
                _source_system,
                _batch_id,
                md5(concat_ws('|',
                    coalesce(nullif(trim(company_name), ''), trim(upper(ticker))),
                    coalesce(nullif(trim(sector), ''), 'Unknown'),
                    coalesce(nullif(trim(industry), ''), 'Unknown'),
                    coalesce(nullif(trim(exchange), ''), 'Unknown'),
                    coalesce(nullif(trim(currency), ''), 'USD'),
                    coalesce(nullif(trim(country), ''), 'Unknown'),
                    coalesce(cast(market_cap AS VARCHAR), '0')
                )) AS attr_hash,
                row_number() OVER (
                    PARTITION BY trim(upper(ticker))
                    ORDER BY cast(_ingested_at AS TIMESTAMP) DESC
                ) AS rn
            FROM read_parquet('{META_GLOB}', hive_partitioning=true)
            WHERE ticker IS NOT NULL
        )
        SELECT * FROM ranked_metadata WHERE rn = 1;

        -- Expire active SCD2 rows where tracked attributes changed
        UPDATE silver.snap_ticker_metadata AS tgt
        SET dbt_valid_to = src._ingested_at
        FROM stg_ticker_metadata AS src
        WHERE tgt.ticker = src.ticker
          AND tgt.dbt_valid_to IS NULL
          AND md5(concat_ws('|',
                tgt.company_name, tgt.sector, tgt.industry,
                tgt.exchange, tgt.currency, tgt.country,
                coalesce(cast(tgt.market_cap AS VARCHAR), '0')
          )) <> src.attr_hash;

        -- Insert new or changed SCD2 versions
        INSERT INTO silver.snap_ticker_metadata
        SELECT
            md5(concat(src.ticker, '|', cast(src._ingested_at AS VARCHAR))) AS dbt_scd_id,
            src.ticker,
            src.company_name,
            src.sector,
            src.industry,
            src.exchange,
            src.currency,
            src.country,
            src.market_cap,
            src._ingested_at,
            src._source_system,
            src._batch_id,
            src._ingested_at AS dbt_valid_from,
            NULL AS dbt_valid_to
        FROM stg_ticker_metadata AS src
        LEFT JOIN silver.snap_ticker_metadata AS active
            ON src.ticker = active.ticker
           AND active.dbt_valid_to IS NULL
        WHERE active.ticker IS NULL;
        """
    )

    # 3. Gold Dimension: gold.dim_assets
    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dim_assets AS
        SELECT
            dbt_scd_id AS asset_sk,
            ticker,
            company_name,
            sector,
            industry,
            exchange,
            currency,
            country,
            market_cap,
            CASE
                WHEN market_cap >= 200000000000 THEN 'Mega Cap'
                WHEN market_cap >= 10000000000 THEN 'Large Cap'
                WHEN market_cap >= 2000000000 THEN 'Mid Cap'
                WHEN market_cap > 0 THEN 'Small Cap'
                ELSE 'Unclassified'
            END AS market_cap_tier,
            dbt_valid_from AS valid_from,
            coalesce(dbt_valid_to, TIMESTAMP '9999-12-31 23:59:59') AS valid_to,
            CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
        FROM silver.snap_ticker_metadata;
        """
    )

    # 4. Gold Dimension: gold.dim_date
    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dim_date AS
        WITH distinct_dates AS (
            SELECT DISTINCT trade_date AS full_date
            FROM silver.silver_market_ohlcv
        )
        SELECT
            cast(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
            full_date,
            extract(year FROM full_date) AS year_num,
            extract(quarter FROM full_date) AS quarter_num,
            extract(month FROM full_date) AS month_num,
            strftime(full_date, '%B') AS month_name,
            extract(day FROM full_date) AS day_of_month,
            extract(isodow FROM full_date) AS iso_day_of_week,
            strftime(full_date, '%A') AS day_name,
            CASE WHEN extract(isodow FROM full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
        FROM distinct_dates;
        """
    )

    # 5. Gold Fact Table: gold.fct_daily_market_trades
    con.execute(
        """
        CREATE OR REPLACE TABLE gold.fct_daily_market_trades AS
        WITH trades AS (
            SELECT
                o.trade_id,
                o.ticker,
                o.trade_date,
                o.open_price,
                o.high_price,
                o.low_price,
                o.close_price,
                o.adj_close_price,
                o.trading_volume,
                o.daily_price_change,
                o.daily_return_pct,
                round(
                    avg(o.close_price) OVER (
                        PARTITION BY o.ticker
                        ORDER BY o.trade_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ),
                    4
                ) AS rolling_7d_avg_close,
                round(o.close_price * o.trading_volume, 2) AS estimated_dollar_volume,
                o._ingested_at,
                o._batch_id
            FROM silver.silver_market_ohlcv o
        )
        SELECT
            t.trade_id,
            coalesce(a.asset_sk, md5(t.ticker)) AS asset_sk,
            d.date_key,
            t.ticker,
            t.trade_date,
            t.open_price,
            t.high_price,
            t.low_price,
            t.close_price,
            t.adj_close_price,
            t.trading_volume,
            t.daily_price_change,
            t.daily_return_pct,
            t.rolling_7d_avg_close,
            t.estimated_dollar_volume,
            t._ingested_at,
            t._batch_id
        FROM trades t
        LEFT JOIN gold.dim_assets a
            ON t.ticker = a.ticker
           AND a.is_current = TRUE
        INNER JOIN gold.dim_date d
            ON t.trade_date = d.full_date;
        """
    )


def run_quality_checks(con: duckdb.DuckDBPyConnection) -> None:
    """Enforce dbt schema.yml data quality contracts on Silver and Gold tables."""
    checks = [
        ("silver.silver_market_ohlcv unique trade_id",
         "SELECT count(*) - count(DISTINCT trade_id) FROM silver.silver_market_ohlcv"),
        ("gold.dim_assets unique asset_sk",
         "SELECT count(*) - count(DISTINCT asset_sk) FROM gold.dim_assets"),
        ("gold.dim_date unique date_key",
         "SELECT count(*) - count(DISTINCT date_key) FROM gold.dim_date"),
        ("gold.fct_daily_market_trades unique trade_id",
         "SELECT count(*) - count(DISTINCT trade_id) FROM gold.fct_daily_market_trades"),
        ("gold.fct_daily_market_trades referential integrity -> dim_date",
         """SELECT count(*) FROM gold.fct_daily_market_trades f
            LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
            WHERE d.date_key IS NULL"""),
        ("gold.fct_daily_market_trades referential integrity -> dim_assets",
         """SELECT count(*) FROM gold.fct_daily_market_trades f
            LEFT JOIN gold.dim_assets a ON f.asset_sk = a.asset_sk
            WHERE a.asset_sk IS NULL"""),
    ]
    for name, sql in checks:
        violations = con.execute(sql).fetchone()[0]
        assert violations == 0, f"Data quality check failed: {name} ({violations} violations)"
        print(f"  [PASS] {name}")


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DB_PATH)) as con:
        print(f"Building Medallion Warehouse -> {DB_PATH}")
        build_warehouse(con)

        print("\nRunning Data Quality Contracts:")
        run_quality_checks(con)

        print("\nTable Row Counts:")
        for tbl in [
            "silver.silver_market_ohlcv",
            "silver.snap_ticker_metadata",
            "gold.dim_assets",
            "gold.dim_date",
            "gold.fct_daily_market_trades",
        ]:
            count = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl:<32} : {count} rows")

        print("\nSample Gold Star Schema Query (Latest Trading Day):")
        sample = con.execute(
            """
            SELECT
                a.ticker,
                a.company_name,
                a.market_cap_tier,
                d.full_date,
                f.close_price,
                f.daily_return_pct,
                f.rolling_7d_avg_close
            FROM gold.fct_daily_market_trades f
            JOIN gold.dim_assets a ON f.asset_sk = a.asset_sk
            JOIN gold.dim_date d ON f.date_key = d.date_key
            WHERE d.full_date = (SELECT max(full_date) FROM gold.dim_date)
            ORDER BY a.ticker
            """
        ).fetchall()
        for row in sample:
            print(" ", row)


if __name__ == "__main__":
    main()
