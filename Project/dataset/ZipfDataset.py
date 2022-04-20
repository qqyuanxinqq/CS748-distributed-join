import sys
from re import L
import numpy as np
# import matplotlib.pyplot as plt
import random
import string
import csv
# the larger par means the data is more skewed

DIR = "/proj/cs784-para-join-PG0/dataset/ZipfDataSet/"
def create_dataset(par, size,filename = 'ZipfDataSet.csv'):
    gfg = np.random.zipf(par, size)
    res = []
    letters = string.ascii_letters + string.digits
    for i in range(size):
        s =  ''.join(random.choice(letters) for i in range(8))
        res.append([gfg[i], s])
    with open(filename, 'w') as f:
        write = csv.writer(f)
        write.writerows(res)
    
    return gfg

def generate_alphas():
    size = 1000000
    for i in range(6):
        for alpha in range(11,26):
            create_dataset(alpha/10,size, filename = DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i))

def generate_sizes():
    alpha = 11
    for i in range(6):
        for size in range(500000,5500000, 500000):
            print(alpha/10,size)
            create_dataset(alpha/10,size, filename = DIR+"Zipf_{}p{}_{}k_{}.csv".format(alpha//10,alpha%10,size//1000,i))
            

if __name__ == "__main__":
    size = 10000
    par = 2.0
    output_file_name = "ZipfDataSet.csv"
    if len(sys.argv) >= 2:
        size = int(sys.argv[1])
    if len(sys.argv) >= 3:
        par = float(sys.argv[2])
    if len(sys.argv) >= 4:
        output_file_name = sys.argv[3]

    create_dataset(par, size, DIR+output_file_name)

# gfg = create_dataset(2.7, 50)
# # print(type(gfg))
# # print(gfg)
# # unique, counts = np.unique(gfg, return_counts=True)

# # print (np.asarray((unique, counts)).T)
# plt.hist(gfg, bins = 50, density = True)
# plt.show()