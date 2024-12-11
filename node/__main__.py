import click
from .node import Node
from .logger import configure_logger


@click.command()
@click.option('--node_id', required=True, help='ID of the Raft node represented by this process')
@click.option('--config', required=True, help='The path to the configuration file of this Raft node',
              type=click.Path(exists=True, file_okay=True, dir_okay=False))
def main(node_id, config):
    configure_logger(node_id)
    raft_node = Node(node_id, config)
    raft_node.run()


if __name__ == '__main__':
    main()  # Ignore "No value for argument" warnings, arguments will be provided by the click
