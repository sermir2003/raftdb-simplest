import click
import json
from .raft import RaftNode
from .http import create_app
from .logger import configure_logging


@click.command()
@click.option('--node_id', required=True, help='ID of the Raft node represented by this process')
@click.option('--config_path', required=True,
              help='The path to the configuration file of this Raft node',
              type=click.Path(exists=True, file_okay=True, dir_okay=False))
def main(node_id, config_path):
    with open(config_path, encoding='utf-8') as file:
        config = json.load(file)
    configure_logging(node_id)
    raft_node = RaftNode(node_id, config)
    app = create_app(raft_node)
    _, port = config['addresses'][node_id].split(':')
    app.run(host='0.0.0.0', port=int(port))


if __name__ == '__main__':
    main()  # Ignore No value for argument, they will be provided by the click
