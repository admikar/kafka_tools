#!/usr/bin/python
import time
import urllib2
import json
import datetime
import os
import sys
import logging
import subprocess
import argparse
import socket
import smtplib
from kafka import KafkaConsumer, KafkaProducer

lagfile = "/home/kafka/mm_lag"
logfile = "/var/log/deployment.log"
parser = argparse.ArgumentParser()
parser.add_argument(
    "-l", "--logfile", help="log file path if not set, is used /var/log/deployment.log"
)
parser.add_argument("-d", "--delay", help="delay in messages count", type=int)
parser.add_argument("-m", "--mails", help="mails list to notifications")

args = parser.parse_args()
if os.path.exists("/home/kafka/.maintenance_mode"):
    exit(0)
if args.logfile:
    logfile = args.logfile


def load_config(file):
    if not os.path.exists(file):
        sys.exit("{0} not found".format(file))
    with open(file, "rt") as f:
        conf = f.read().splitlines()
        conf_dict = dict()
        for elem in conf:
            conf_dict[elem.split("=", 1)[0]] = elem.split("=", 1)[1].split(",")
        return conf_dict

logging.basicConfig(filename=logfile, level=logging.CRITICAL, format="%(message)s")
mirrors = load_config("/home/kafka/mirrormaker/consumer.properties")[
    "bootstrap.servers"
]
centers = load_config("/home/kafka/mirrormaker/producer.properties")[
    "bootstrap.servers"
]
if "wgie" in mirrors[-1]:
    region = "wgie"
else:
    region = mirrors[-1].split("-")[0]
topic ='wgdp-kafka-production-'+region
try:
    producer = KafkaProducer(bootstrap_servers=mirrors)
    message = bytes(str(int(time.time())).encode("utf-8"))
    producer.send(topic, message)
except:
    current_time = datetime.datetime.now().isoformat()
    hostname = os.uname()[1].split(".")[0]
    logging.critical(
        "{} {} No available brokers to check".format(current_time, hostname)
    )

consumer = KafkaConsumer(topic, group_id='mmchecker', bootstrap_servers=centers, consumer_timeout_ms=15000, auto_commit_interval_ms=4000)
if not os.path.exists(lagfile):
    f = open(lagfile, "w")
    f.write("0")
f = open(lagfile, "r+")
prelag = int(f.read())
messages = []
for msg in consumer:
    messages.append(msg.value)
messages.sort()
if len(messages) > 0:
    c_time = int(time.time())
    delay = int((c_time - int(messages[-1])) / 60 / 5)
else:
    delay = prelag + 1
f.seek(0)
f.write(str(delay))
f.truncate()
subprocess.call(
    'echo "wgdp.kafka.mirrormaker.{}.delay_in_messages {} `date +%s`" | nc carbon.wgdp.io 2003'.format(
        region, delay
    ),
    shell=True,
)
if delay > args.delay:
    hostname = os.uname()[1].split(".")[0]
    event = {
        "what": "Restart mm at {}".format(region),
        "tags": "{},restartmm".format(region),
    }
    req = urllib2.Request(
        "http://graphite-web.wgdp.io:8080/events/",
        data=json.dumps(event),
        headers={"Content-type": "text/plain"},
    )
    r = urllib2.urlopen(req)
    r.read()
    sender = "kafka@" + socket.gethostname()
    receivers = args.mails.split(",")
    message = """From: From Mirrormaker checker <{0}>
        To: To notifiers <{1}>
        Subject: Mirrormaker restarted at {2}
        Mirrormaker restarted at {2}""".format(
        sender, receivers, socket.gethostname()
    )
    mail = smtplib.SMTP("localhost")
    mail.sendmail(sender, receivers, message)
    current_time = datetime.datetime.now().isoformat()
    hostname = os.uname()[1].split(".")[0]
    logging.critical(
        "{} {} MirrorMaker: CRITICAL have problems, restart mirrormaker".format(
            current_time, hostname
        )
    )
    subprocess.call("sudo /usr/bin/supervisorctl restart kafka-mirrormaker", shell=True)
    sys.exit("Mirror maker restarted")

