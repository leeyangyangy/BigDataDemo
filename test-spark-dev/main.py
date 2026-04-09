from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# import socket

# =========================
# 获取本机 IP（自动适配）
# =========================
# def get_local_ip():
#     s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#     try:
#         s.connect(("8.8.8.8", 80))
#         ip = s.getsockname()[0]
#     finally:
#         s.close()
#     return ip


# local_ip = get_local_ip()
# print(f"Driver IP: {local_ip}")

# =========================
# 创建 SparkSession
# =========================
spark = SparkSession.builder \
    .appName("RemoteSparkTest") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "spark-master") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.port", "4040") \
    .config("spark.blockManager.port", "4040") \
    .getOrCreate()

# .config("spark.eventLog.enabled", "true")
# .config("spark.eventLog.dir", "file:///tmp/spark-events")
print("Spark Version:", spark.version)

# =========================
# 测试 1：基础计算（验证集群执行）
# =========================
print("\n=== Test 1: Range Count ===")
df = spark.range(1, 1000000)
count = df.count()
print("Count Result:", count)

# =========================
# 测试 2：DataFrame 操作
# =========================
print("\n=== Test 2: DataFrame ===")
data = [
    ("A", 10),
    ("A", 20),
    ("B", 30),
    ("B", 40),
    ("C", 50),
]

df2 = spark.createDataFrame(data, ["category", "value"])

df2.show()

# 聚合
result = df2.groupBy("category").agg(avg(col("value")).alias("avg_value"))
result.show()

# =========================
# 测试 3：SQL 查询
# =========================
print("\n=== Test 3: SQL ===")

df2.createOrReplaceTempView("test_table")

sql_result = spark.sql("""
    SELECT category, COUNT(*) as cnt, AVG(value) as avg_val
    FROM test_table
    GROUP BY category
""")

sql_result.show()

# =========================
# 测试 4：并行度（确认用到 Worker）
# =========================
print("\n=== Test 4: Parallelism ===")
rdd = spark.sparkContext.parallelize(range(1000000), 8)
print("Partitions:", rdd.getNumPartitions())
print("Sum:", rdd.sum())

# =========================
# 结束
# =========================
spark.stop()
print("\n=== DONE ===")
