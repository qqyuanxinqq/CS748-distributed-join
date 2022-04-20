#pyspark --master spark://128.105.144.80:7078 <AlphaTest.py >results/test2.txt 2>&1
import equijoin

equijoin.test1("broadcast")
equijoin.test1("merge")
# equijoin.test1("SHUFFLE_HASH")