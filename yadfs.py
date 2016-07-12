#!/usr/bin/env python3.5
import click
from client.client import Client
from utils.enums import Status, NodeType
import getpass
import datetime


# CLI
@click.group(invoke_without_command=False)
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument('path', default="/")
def ls(path):
    """List directory contents"""
    cl = Client()
    dir_ls = cl.list_dir(path)
    stat = dir_ls['status']
    if stat == Status.ok:
        for item, info in dir_ls['items'].items():
            fr = "-rw-r--r--"
            if info['type'] == NodeType.directory:
                fr = "drwxr-xr-x"
            date = datetime.datetime.fromtimestamp(info['date'])
            date_format = '%b %d %H:%M' if date.year == datetime.datetime.today().year else '%b %d %Y'
            print('%.11s   %.10s   %6sB   %.15s    %s' % (fr,
                                                       getpass.getuser(),
                                                       info['size'],
                                                       datetime.datetime.fromtimestamp(info['date']).strftime(date_format),
                                                       item))
    else:
        print(Status.description(stat))


@cli.command()
@click.argument('path')
def mkdir(path):
    """Create a directory"""
    cl = Client()
    res = cl.create_dir(path)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))


@cli.command()
@click.argument('path')
def rmdir(path):
    """Remove a directory"""
    cl = Client()
    res = cl.delete_dir(path)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))


@cli.command()
@click.argument('local_path')
@click.argument('remote_path', default="/")
def upload(local_path, remote_path):
    """Create a file"""
    cl = Client()
    res = cl.create_file(local_path, remote_path)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))


@cli.command()
@click.argument('path')
def rm(path):
    """Delete a file"""
    cl = Client()
    res = cl.delete_file(path)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))


@cli.command()
@click.argument('path')
def status(path):
    """Check if path refers to file or directory"""
    cl = Client()
    res = cl.path_status(path)
    print('%s is a %s' % (path, NodeType.description(res['type'])))


@cli.command()
@click.argument('path_from')
@click.argument('path_to')
def download(path_from, path_to):
    """Download a file"""
    cl = Client()
    res = cl.download_file(path_from, path_to)
    # print(res)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))


@cli.command()
@click.argument('path')
def cat(path):
    """Print a file"""
    cl = Client()
    res = cl.get_file_content(path)
    print(res)

if __name__ == '__main__':
    cli()
