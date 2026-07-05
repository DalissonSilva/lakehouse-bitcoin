SELECT * EXCEPT (rn) FROM (
    SELECT
        open_time, close_time, open, high, low, close, volume,
        num_trades, fechada, source_symbol,
        DATE(open_time) AS data_referencia,
        YEAR(open_time) AS ano,
        MONTH(open_time) AS mes,
        ROW_NUMBER() OVER (PARTITION BY open_time ORDER BY ingest_ts_utc DESC) AS rn
    FROM {{ source('bronze', 'btc_ohlcv') }}
)
WHERE rn = 1
