CREATE TABLE `${TABLE_NAME}` (
  `f1` String,
  `f2` bigint,
  `f3` int
) with (
--   'connector' = 'print'
  'connector' = 'clickhouse',
  'url' = 'clickhouse://127.0.0.1:8123',
  'database-name' = 'default',
  'table-name' = '${TABLE_NAME}_dist',
  'sink.batch-size' = '2000',
  'sink.backpressure-aware' = 'true'
);