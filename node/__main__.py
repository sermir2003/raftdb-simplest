import click
from .raft import RaftNode
from .http import create_app
from .logger import configure_logging


@click.command()
@click.option('--id', required=True, help='ID of the Raft node represented by this process')
@click.option('-c', '--config_path', required=True,
              help='The path to the configuration file of this Raft node',
              type=click.Path(exists=True, file_okay=True, dir_okay=False))
def main(node_id, config_path):
    configure_logging(node_id)
    raft_node = RaftNode(id, config_path)
    app = create_app(raft_node)
    app.run(debug=True)


if __name__ == '__main__':
    main()  # Ignore No value for argument, they will be provided by the click
