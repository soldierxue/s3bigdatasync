import logging
from logging.config import fileConfig
from daemonize import Daemonize
from etag_app import main
import conf
import sys

if __name__ == "__main__":

    fileConfig('./logging_config.ini')

    input = sys.argv[1]
    print('start to process input file', input)
    main(input)
    #daemon = Daemonize(app="etag_checker", pid=conf.PID, action=main, auto_close_fds=False)
    #daemon.start()
