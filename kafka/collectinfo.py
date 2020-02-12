#!/usr/bin/python
from kazoo.client import KazooClient
from subprocess import check_output
import argparse
import json
import socket
import time

def define_region_name(host):
    if "edw" in host:
        region = "edcenter"
    elif "cn1" in host:
        region = "cnn"
    elif "cn4" in host:
        region = "cns"
    elif "wgie" in host:
        region = "wgie"
    else:
        region = host.split("-")[0]
    return region


def parse_config(project):
    if project == "kafka":
        config = "/etc/kafka/server.properties"
    elif project == 'schema-registry':
        config = "/etc/schema-registry/schema-registry.properties"
    with open(config, "rt") as f:
        conf = list()
        conf_dict = dict()
        for line in f:
            if not line.startswith("#") and line != ("\n"):
                conf.append(line)
        for elem in conf:
            conf_dict[elem.split("=", 1)[0]] = elem.split("=", 1)[1].rstrip("\n")
        return conf_dict


def collect_info(project):
    conf_dict = parse_config(project)
    conf_dict["hostname"] = socket.gethostname().split(".")[0]
    conf_dict["version"] = check_output(
        "rpm -qa | grep confluent-{}".format(project), shell=True
    ).rstrip("\n")
    conf_dict["time"] = time.time()
    return conf_dict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--project", help="project name kafka or schema-registry", default="kafka"
    )
    parser.add_argument(
        "-z",
        "--zookeeper",
        help="zookeeper url with port",
        default='zookeeper.wgdp.io:2181',
    )
    args = parser.parse_args()
    conf_dict = collect_info(args.project)
    print(conf_dict)
    region = define_region_name(conf_dict["hostname"])
    if region == 'edcenter':
        if args.project == 'kafka':
            conf_dict['url'] = 'kafka.wgdp.io'
        else:
            conf_dict['url'] = 'schemas-registry.wgdp.io'
    else:
        conf_dict['url'] = '{}-kafka.wgdp.io'.format(region)
    zk = KazooClient(hosts=args.zookeeper)
    zk.start()
    zk.ensure_path("/app/{}/{}/{}".format(args.project, region, conf_dict["hostname"]))
    zk.set(
        "/app/{}/{}/{}".format(args.project, region, conf_dict["hostname"]),
        json.dumps(conf_dict),
    )


if __name__ == "__main__":
    main()
