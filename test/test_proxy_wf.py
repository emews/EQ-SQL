import proxies
from math import sin


# this function is proxied in the ME, and proxies.app decorator
# unpacks the arguments.
@proxies.app
def task_func(c, x, y):
    result = c * sin(4 * x) + sin(4 * y) + -2 * x + x**2 - 2 * y + y**2
    return result
