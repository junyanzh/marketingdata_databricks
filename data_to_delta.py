# 轉換 ga4_july.parquet 為 Delta 表
file_location_ga4 = "/FileStore/tables/ga4_july.parquet"
df_ga4_july = spark.read.format("parquet").load(file_location_ga4)
delta_path_ga4 = "/delta/ga4_july_delta"
df_ga4_july.write.format("delta").save(delta_path_ga4)

# 轉換 order_july.csv 為 Delta 表
file_location_order = "/FileStore/tables/order_july.csv"
df_order_july = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("sep", ",") \
  .load(file_location_order)
delta_path_order = "/delta/order_july_delta"
df_order_july.write.format("delta").save(delta_path_order)

# 轉換 pagespeedinsight_2023_07_02.json 為 Delta 表
file_location_pagespeed = "/FileStore/tables/pagespeedinsight_0730.json"
df_pagespeedinsight = spark.read.format("json").load(file_location_pagespeed)
delta_path_pagespeed = "/delta/pagespeedinsight_delta"
df_pagespeedinsight.write.format("delta").save(delta_path_pagespeed)

# 轉換 searchconsole_july.csv 為 Delta 表
file_location_searchconsole = "/FileStore/tables/searchconsole_july.csv"
df_searchconsole_july = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("sep", ",") \
  .load(file_location_searchconsole)
delta_path_searchconsole = "/delta/searchconsole_july_delta"
df_searchconsole_july.write.format("delta").save(delta_path_searchconsole)

# 將 Delta 路徑與 Delta 表名稱關聯
spark.sql(f"""
CREATE TABLE ga4_july_delta
USING DELTA
LOCATION '{delta_path_ga4}'
""")

spark.sql(f"""
CREATE TABLE order_july_delta
USING DELTA
LOCATION '{delta_path_order}'
""")

spark.sql(f"""
CREATE TABLE pagespeedinsight_delta
USING DELTA
LOCATION '{delta_path_pagespeed}'
""")
spark.sql(f"""
CREATE TABLE searchconsole_july_delta
USING DELTA
LOCATION '{delta_path_searchconsole}'
""")
