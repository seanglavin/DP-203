import logging

logger = logging.getLogger("app")
logging.basicConfig(level=logging.INFO)

file_handler = logging.FileHandler('logfile.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s.%(module)s : %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)