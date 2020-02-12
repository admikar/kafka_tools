#!/bin/bash
set -e
BROKERS="`grep zookeeper.connect /etc/kafka/server.properties | awk -F"=" '{print $2}'| sed -e 's/\:2181/\:9092/g'`"
ZOOKEEPER="`grep zookeeper.connect /etc/kafka/server.properties | awk -F"=" '{print $2}'`"
declare -a topics=($(kafka-topics --zookeeper ${ZOOKEEPER} --list | tr "\n" " "))
for topic in ${topics[@]}
  do
    messages=$(kafka-log-dirs --bootstrap-server ${BROKERS} --topic-list $topic --describe | grep '^{' | jq '[ ..|.size? | numbers ] | add')
    partitions=$(kafka-topics --zookeeper ${ZOOKEEPER} --describe --topic $topic | head -n1 | awk -F" " '{print $2}' | awk -F":" '{print $2}')
    echo "$topic has $messages messages and $partitions partitions" >> /var/log/kafka/kafka-unused-topics.log.`date +"%Y-%m-%d"`
    if [ $messages -eq 0 -a $partitions -gt 1 ]; then
        kafka-topics --delete --zookeeper ${ZOOKEEPER} --topic $topic
        echo "$topic deleted" >> /var/log/kafka/kafka-unused-topics.log.`date +"%Y-%m-%d"`
        sleep 60
    fi
done
