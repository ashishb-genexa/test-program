import logging

def set_get_logger(logger_name,log_file):
	# Create Logger
	logger = logging.getLogger(logger_name)
	logger.setLevel(logging.DEBUG)

	# Create a file handler
	file_handler = logging.FileHandler(log_file)
	#file_handler.setLevel(logging.DEBUG)

	# Create Stream handler
	stream_handler = logging.StreamHandler()
	#stream_handler.setLevel(logging.DEBUG)

	# Create a formatter and set it for the handler
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%b-%d %H:%M:%S')
	file_handler.setFormatter(formatter)
	stream_handler.setFormatter(formatter)

	logger.addHandler(file_handler)
	logger.addHandler(stream_handler)
	return logger
