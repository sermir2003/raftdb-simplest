import logging


logger = logging.getLogger()

def configure_logging(node_id):
    logging.basicConfig(
        level=logging.DEBUG,
        format=f'({node_id}) - %(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S,%f',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'node-{node_id}.log', mode='a')
        ]
    )
