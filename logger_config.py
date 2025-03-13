import logging

logger = logging.getLogger("DP-203")
logging.basicConfig(level=logging.INFO)

file_handler = logging.FileHandler('logfile.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s.%(module)s : %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)