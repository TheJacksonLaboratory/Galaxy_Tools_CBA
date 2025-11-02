"""
        This module handles the logging for dcc_xml generator
"""


import logging
from logging.handlers import TimedRotatingFileHandler
import re

from datetime import datetime

g_logger = None

def init():
    
    log_file = '/projects/galaxy/tools/cba/log/aws_log'
    
    global g_logger
    g_logger = logging.getLogger(__name__)
    date = datetime.now().strftime("%B-%d-%Y")
    #FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
    FORMAT = "[%(asctime)s]%(levelname)s: %(message)s"
    logging.basicConfig(format=FORMAT, filemode="w", level=logging.WARNING, force=True)
    handler =TimedRotatingFileHandler(f"{log_file}_{date}.log" , when="midnight", backupCount=10)
    handler.setFormatter(logging.Formatter(FORMAT))
    handler.suffix = "%Y%m%d"
    handler.extMatch = re.compile(r"^\d{8}$")
    g_logger.addHandler(handler)
    
    g_logger.info('Logger has been created')

def info(message):
    global g_logger
    if g_logger == None:
        init()
        
    g_logger.info(message)
    

def debug(message):
    global g_logger
    if g_logger == None:
        init()
        
    g_logger.debug(message)

def error(message):
    global g_logger
    if g_logger == None:
        init()
        
    g_logger.error(message)
    