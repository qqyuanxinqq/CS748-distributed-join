from re import L
import numpy as np
import matplotlib.pyplot as plt
import random
import string
import csv
# the larger par means the data is more skewed
def create_dataset(par, size):
    gfg = np.random.zipf(par, size)
    res = []
    letters = string.ascii_letters + string.digits
    for i in range(size):
        s =  ''.join(random.choice(letters) for i in range(8))
        res.append([gfg[i], s])
    with open('dataset', 'w') as f:
        write = csv.writer(f)
        write.writerows(res)
    
    return gfg

gfg = create_dataset(2.7, 50)
# print(type(gfg))
# print(gfg)
# unique, counts = np.unique(gfg, return_counts=True)

# print (np.asarray((unique, counts)).T)
plt.hist(gfg, bins = 50, density = True)
plt.show()