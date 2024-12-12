import logging


logger = logging.getLogger()

def configure_logger(node_id):
    logging.basicConfig(
        level=logging.INFO,
        format=f'{node_id} - %(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'{node_id}/log.log', mode='a')
        ]
    )
