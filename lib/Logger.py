from datetime import datetime
from lib import Config
import logging
from logging.handlers import QueueHandler
conf = Config.conf


def time_str():
    return datetime.now().strftime("%d/%m %H:%M:%S")
def isDEVmode():
    if conf.DEV_MODE == 'Y':
        return True
    return False
class log:
    logger = logging.getLogger()
    logger.setLevel(conf.LOG_level)
    formatter = logging.Formatter(conf.LOG_format)
    streamLogHandler = logging.StreamHandler()
    streamLogHandler.setFormatter(formatter)
    logger.addHandler(streamLogHandler)
    logFileHandler = logging.handlers.RotatingFileHandler(
        filename=conf.LOG_file_dir + 'app.log',
        maxBytes=conf.LOG_max_byte_size,
        backupCount=conf.LOG_file_count,
        mode='a'
    )
    logFileHandler.setFormatter(formatter)
    logger.addHandler(logFileHandler)
    def __init__(self):
        pass

    def success (self,message):
        self.logger.info(message)
        if isDEVmode():
            ts = time_str()
            print(f"\033[92m{ts} | {message}\033[0m")
    def warning (self,message):
        self.logger.warning(message)
        if isDEVmode():
            ts = time_str()
            print(f"\033[33m{ts} | {message}\033[0m")
    def error (self,message):
        self.logger.error(message)
        if isDEVmode():
            ts = time_str()
            print(f"\033[91m{ts} | {message}\033[0m")
    def info (self,message):
        self.logger.info(message)
        if isDEVmode():
            ts = time_str()
            print(f"\033[94m{ts} | {message}\033[0m")
    def debug(self,message):
        self.logger.debug(message)
        if isDEVmode():
            if conf.LOG_level == 'DEBUG':
                ts = time_str()
                print(f"\033[97m{ts} | DEBUG | {message}\033[0m")