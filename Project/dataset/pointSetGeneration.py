import sys
import numpy as np

if __name__ == "__main__":
    point_num = 10000
    output_file_name = "pointDataSet.csv"
    if len(sys.argv) >= 2:
        point_num = int(sys.argv[1])

    if len(sys.argv) >= 3:
        output_file_name = sys.argv[2]

    arr = np.random.rand(point_num, 1)
    np.savetxt(output_file_name, arr, delimiter=",")
