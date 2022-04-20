def timer(filename):
    def helper(func):
        output = filename
        def func_wrapper(*args, **kwargs):
            from time import time
            time_start = time()
            result = func(*args, **kwargs)
            time_end = time()
            time_spend = time_end - time_start
            with open(output, 'a') as f:
                f.write('%.3f,' % (time_spend))
                print('%s cost time: %.3f s' % (func.__name__, time_spend))
            return result
        return func_wrapper
    return helper