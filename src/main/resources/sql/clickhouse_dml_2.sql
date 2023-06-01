INSERT INTO `${TABLE_NAME}`
SELECT cast(b.f1 as bigint),b.f2,b.f3,b.f4 FROM (
  SELECT * FROM `kafka_source` WHERE f0 = '${TABLE_NAME}'
) AS a JOIN LATERAL TABLE(${UDTF_NAME}(a.f1)) AS b(f1, f2, f3, f4) ON TRUE;
