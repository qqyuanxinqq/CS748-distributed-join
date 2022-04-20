from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from utils.utils import timer
from time import time

DIR = "/proj/cs784-para-join-PG0/dataset/ZipfDataSet/"


spark_conf = SparkConf()
spark_conf.setAll([
    ("spark.cores.max", "20"),
    ('spark.master', 'spark://128.105.144.80:7078'),
    # ('spark.master', 'local[*]'),
    ('spark.app.name', "Test"),
    ('spark.submit.deployMode', 'client'),
    ('spark.ui.showConsoleProgress', 'true'),
    ('spark.eventLog.enabled', 'false'),
    ('spark.logConf', 'false'),
#     ('spark.driver.bindAddress', '0.0.0.0'), # <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
#     ('spark.driver.host', '0.0.0.0'), # <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
])

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
sc = spark.sparkContext

# rdd1 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(1,1,500,1)).rdd.sortByKey(True,40)
# rdd2 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(1,1,500,2)).rdd.sortByKey(True,40)

rdd1 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(1,1,500,1)).repartition(160)
rdd2 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(1,1,500,2)).repartition(160)

time_start = time()
t1 = rdd1.join(rdd2)
t1.collect()
time_end = time()
time_spend = time_end - time_start
print(time_spend)

n1 = rdd1.map(lambda s:(s[0],1)).reduceByKey(lambda a,b: a+b)
# n1 = sc.broadcast(rdd1.countByKey())

# temp2 = temp1.reduceByKey(lambda a,b: a+b)
# temp3 = temp2.map(lambda s:s[1]).reduce(lambda a,b: a+b)


r1 = rdd1.join(rdd2.hint("broadcast"),'_c0')
r1.count()


sdf.rdd.join(sdf2.rdd)
sdf.rdd.getNumPartitions()



data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)