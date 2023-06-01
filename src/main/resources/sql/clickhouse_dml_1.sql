INSERT INTO `${TABLE_NAME}`
SELECT b.f1,cast(b.f2 as bigint),cast(b.f3 as int) FROM (
  SELECT * FROM `kafka_source` WHERE f0 = '${TABLE_NAME}'
) AS a JOIN LATERAL TABLE(${UDTF_NAME}(a.f1)) AS b(f1, f2, f3) ON TRUE;
