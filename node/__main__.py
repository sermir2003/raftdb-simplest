import click
from raft import RaftNode
from http import create_app


@click.command()
@click.argument('id', type=click.Int())
@click.argument('config_path', type=click.Path(exists=True, file_okay=True, dir_okay=False))
def main(id, config_path):
    raft_node = RaftNode(id, config_path)
    app = create_app(raft_node)
    app.run(debug=True)


if __name__ == '__main__':
    main()
