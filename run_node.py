#!/usr/bin/env python3
import os
import json
import click


@click.command()
@click.argument('node_id')
def main(node_id):
    config = 'config.json'
    config_absolute_path = os.path.abspath(config)
    with open(config, encoding='utf-8') as file:
        config_json = json.load(file)
    port = config_json['addresses'][node_id].split(':')[-1]
    current_location = os.path.dirname(os.path.abspath(__file__))
    folder_on_host = current_location + '/' + node_id
    os.makedirs(folder_on_host, exist_ok=True)
    args = ["docker", "run",
            "--rm",
            "-p", f"{port}:{port}",
            "-v", f"{config_absolute_path}:/app/config.json",
            "-v", f"{folder_on_host}:/app/{node_id}",
            "--name", node_id,
            "--network", "raft_network",
            "node_image",
            "--node_id", f"{node_id}",
            "--config", "/app/config.json"]
    print('command: ' + ' '.join(args))
    os.execvp("docker", args)


if __name__ == '__main__':
    main()  # Ignore "No value for argument" warning, they will be provided by the click
