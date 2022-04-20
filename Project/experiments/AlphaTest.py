#pyspark --master spark://128.105.144.80:7078 <AlphaTest.py >results/test2.txt 2>&1
import equijoin

equijoin.test2("broadcast")
equijoin.test2("merge")