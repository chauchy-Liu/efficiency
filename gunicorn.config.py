import multiprocessing

# debug = True
# loglevel = 'debug'
# accesslog = "log/access.log"
# errorlog = "log/debug.log"

bind = "0.0.0.0:8889"
worker_class = 'gevent'

# 启动的进程数
workers = 1