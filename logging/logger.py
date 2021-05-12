import logging

    
logger = logging.getLogger('retail-etl-info')
formatter = logging.Formatter('%(asctime)s [%(message)s]')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
fh = logging.FileHandler('retail-etl.log')
logger.addHandler(fh)
logger.addHandler(ch)