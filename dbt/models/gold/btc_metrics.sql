SELECT
    open_time,
    data_referencia,
    close,
    volume,
    ROUND(close - LAG(close) OVER (ORDER BY open_time), 2) AS variacao_absoluta,
    ROUND((close / LAG(close) OVER (ORDER BY open_time) - 1) * 100, 4) AS retorno_pct,
    ROUND(AVG(close) OVER (ORDER BY open_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS media_movel_7d,
    ROUND(AVG(close) OVER (ORDER BY open_time ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS media_movel_30d,
    ROUND(STDDEV(close) OVER (ORDER BY open_time ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS volatilidade_30d,
    ROUND((close / MAX(close) OVER (ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - 1) * 100, 2) AS drawdown_pct
FROM {{ ref('btc_ohlcv_clean') }}
WHERE fechada = true
ORDER BY open_time
