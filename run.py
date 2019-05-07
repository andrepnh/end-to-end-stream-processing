#!/usr/bin/env python3
import os
import subprocess
import time
import urllib.request, urllib.parse

def require(cond, message):
    if not cond:
        raise Exception(message)

def execute(command):
    subprocess.run(command.split(' ')).check_returncode()

def send(url, body='', method='GET', headers={}):
    try:
        if method in ('POST', 'PUT'):
            if body and not 'Content-Type' in headers:
                headers['Content-Type'] = 'application/json'
            req = urllib.request.Request(url, body.encode('utf-8'),
                                         headers=headers, method=method)
            return urllib.request.urlopen(req)
        elif method == 'DELETE':
            req = urllib.request.Request(url, method='DELETE')
            return urllib.request.urlopen(req)
        else:
            return urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        return e.file

def read(path):
    contents = ''
    with open(path) as file:
        for line in file:
            contents += line.strip()
    return contents

def wait_for(cond, attempts=10, max_wait=10000, base=100):
    for attempt in range(1, attempts + 1):
        if cond():
            return
        wait = min(max_wait, base * 2 ** attempt)
        if attempt != attempts:
            print('Attempt {} for {} not successful; will try again in {}ms'
                  .format(attempt, str(cond).split()[1], wait))
            time.sleep(wait / 1000)
    raise Exception('Failed after all attempts')

def es_available():
    try:
        response = send('http://{}:9200'.format(os.environ['DOCKER_HOST_IP']))
        return response.status == 200
    except:
        return False

def connect_available():
    try:
        response = send('http://{}:8083'.format(os.environ['DOCKER_HOST_IP']))
        return response.status == 200
    except:
        return False

def kibana_available():
    try:
        response = send('http://{}:5601'.format(os.environ['DOCKER_HOST_IP']))
        return response.status == 200
    except:
        return False

def create_es_index_template():
    url = ('http://{}:9200/_template/inventory-template'
           .format(os.environ['DOCKER_HOST_IP']))
    body = read('./elasticsearch/index-templates.json')
    send(url, body, method='PUT')

def create_connectors():
    url = 'http://{}:8083/connectors'.format(os.environ['DOCKER_HOST_IP'])
    conf_dir = os.path.join('connect', 'config.d')
    for json in os.listdir(conf_dir):
        resp = send(url, read(os.path.join(conf_dir, json)), method='POST')
        require(200 <= resp.code < 300 or resp.code == 409,
                'Unexpected status when creating connector: {}'
                .format(resp.code))

def create_kibana_objects():
    url = ('http://{}:5601/api/saved_objects/_bulk_create'
           .format(os.environ['DOCKER_HOST_IP']))
    body = read('./elasticsearch/kibana-objects.json')
    resp = send(url, body, method='POST', headers={'kbn-xsrf': 'reporting'})
    require(200 <= resp.code < 300,
            'Unexpected response when creating kibana objects: {}'
            .format(resp.code))

if __name__ == "__main__":
    require('DOCKER_HOST_IP' in os.environ,
            'The environment variable DOCKER_HOST_IP should be defined')
    execute('docker-compose build')
    print('Starting postgres, zookeeper, kafka and elasticsearch...')
    execute('docker-compose up -d postgres zookeeper kafka elasticsearch')

    print('Waiting for elasticsearch to go up...')
    wait_for(es_available, attempts=20) # Elasticsearch can take a while
    print('Creating elasticsearch index templates...')
    create_es_index_template()

    print('Starting Kafka Connect')
    execute('docker-compose up -d connect')
    print('Waiting for kafka connect to go up...')
    wait_for(connect_available)
    print('Creating connectors...')
    create_connectors()

    print('Starting inventory generator and kibana...')
    execute('docker-compose up -d generator kibana')
    print('Waiting for kibana to go up...')
    wait_for(kibana_available, attempts=30) # Kibana can take a long time
    print('Creating kibana objects...')
    create_kibana_objects()

    print('Starting Kafka Streams app...')
    execute('docker-compose up -d streams')
    print('Stream processing will happen on the background, go to http://{}:5601 '
          + 'to see the live dashboard'
          .format(os.environ['DOCKER_HOST_IP']))
