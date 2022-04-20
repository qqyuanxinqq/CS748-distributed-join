from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from utils.utils import timer


DIR = "/proj/cs784-para-join-PG0/dataset/ZipfDataSet/"

RESULT = "results/"

def test1(hint, testname = "_input_size2.txt"):
    
    testname = hint+testname
    alpha = 11

    @timer(RESULT +  testname)
    def join_on_size(df1,df2):
        r1 = df1.join(df2.hint(hint),'_c0')
        r1.count()
        return r1
    spark_conf = SparkConf()
    spark_conf.setAll([
        ("spark.cores.max", "20"),
        ('spark.master', 'spark://128.105.144.80:7078'),
        # ('spark.master', 'local[*]'),
        ('spark.app.name', testname),
        ('spark.submit.deployMode', 'client'),
        ('spark.ui.showConsoleProgress', 'true'),
        ('spark.eventLog.enabled', 'false'),
        ('spark.logConf', 'false'),
    #     ('spark.driver.bindAddress', '0.0.0.0'), # <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
    #     ('spark.driver.host', '0.0.0.0'), # <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
    ])

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    
    with open(RESULT + testname, 'a') as f:
        f.write("Alpha: {}p{}".format(alpha//10,alpha%10))
    for size in range(500000,5500000, 500000):
        with open(RESULT + testname, 'a') as f:
            f.write("\n{},".format(size))
        for i in range(5):
            sdf = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i)).repartition(40)
            sdf2 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i+1)).repartition(40)
            join_on_size(sdf,sdf2)
    spark.stop()
    print("finished")


def test2(hint , testname = "_alpha2.txt"):
    testname = hint+testname
    size = 1000000
    
    @timer(RESULT +  testname)
    def join_on_alpha(df1,df2):
        r1 = df1.join(df2.hint(hint),'_c0')
        r1.count()
        return r1
    
    spark_conf = SparkConf()
    spark_conf.setAll([
        ("spark.cores.max", "20"),
        ('spark.master', 'spark://128.105.144.80:7078'),
        # ('spark.master', 'local[*]'),
        ('spark.app.name', testname),
        ('spark.submit.deployMode', 'client'),
        ('spark.ui.showConsoleProgress', 'true'),
        ('spark.eventLog.enabled', 'false'),
        ('spark.logConf', 'false'),
    #     ('spark.driver.bindAddress', '0.0.0.0'), # <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
    #     ('spark.driver.host', '0.0.0.0'), # <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
    ])

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    with open(RESULT +  testname, 'a') as f:
        f.write("Size: {}k ".format(size//1000))
    
    for alpha in range(11,26):
        with open(RESULT +  testname, 'a') as f:
            f.write("\n{}.{},".format(alpha//10,alpha%10))
        for i in range(5):
            sdf = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i)).repartition(40)
            sdf2 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i+1)).repartition(40)
            join_on_alpha(sdf,sdf2)
    spark.stop()
    print("finished")

def test3(hint, testname = "_cores.txt"):
    testname = "cores_"+hint+".txt"
    size = 1000000
    alpha = 11
    with open(RESULT +  testname, 'a') as f:
        f.write("Size: {}k alpha: {}p{}".format(size//1000,alpha//10,alpha%10))
    @timer(RESULT +  testname)
    def join_on_cores(df1,df2):
        r1 = df1.join(df2.hint(hint),'_c0')
        r1.count()
        return r1
    for cores in range(2,32,2):
        spark_conf = SparkConf()
        spark_conf.setAll([
            ("spark.cores.max", str(cores)),
            ('spark.master', 'spark://128.105.144.80:7078'),
            # ('spark.master', 'local[*]'),
            ('spark.app.name', testname),
            ('spark.submit.deployMode', 'client'),
            ('spark.ui.showConsoleProgress', 'true'),
            ('spark.eventLog.enabled', 'false'),
            ('spark.logConf', 'false'),
        ])

        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

        with open(RESULT +  testname, 'a') as f:
            f.write("\n{},".format(cores))
        for i in range(5):
            sdf = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i)).repartition(40)
            sdf2 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i+1)).repartition(40)
            join_on_cores(sdf,sdf2)
        spark.stop()
    
    print("finished")


def test1(hint, testname = "_input_size_semijoin.txt"):
    
    testname = hint+testname
    alpha = 11

    @timer(RESULT +  testname)
    def join_on_size(df1,df2):
        hashmap = df1.rdd.collectAsMap()
        df2 = df2.rdd.filter(lambda x: x[0] in hashmap)
        df2 = spark.createDataFrame(df2)
        r1 = df1.join(df2.hint(hint),'_c0')
        
        r1.count()
        return r1
    spark_conf = SparkConf()
    spark_conf.setAll([
        ("spark.cores.max", "20"),
        ('spark.master', 'spark://128.105.144.80:7078'),
        # ('spark.master', 'local[*]'),
        ('spark.app.name', testname),
        ('spark.submit.deployMode', 'client'),
        ('spark.ui.showConsoleProgress', 'true'),
        ('spark.eventLog.enabled', 'false'),
        ('spark.logConf', 'false'),
    #     ('spark.driver.bindAddress', '0.0.0.0'), # <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
    #     ('spark.driver.host', '0.0.0.0'), # <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
    ])

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    
    with open(RESULT + testname, 'a') as f:
        f.write("Alpha: {}p{}".format(alpha//10,alpha%10))
    for size in range(500000,5500000, 500000):
        with open(RESULT + testname, 'a') as f:
            f.write("\n{},".format(size))
        for i in range(5):
            sdf = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i)).repartition(40)
            sdf2 = spark.read.csv(DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i+1)).repartition(40)
            join_on_size(sdf,sdf2)
    spark.stop()
    print("finished")