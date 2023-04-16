#!/bin/bash

while ! nc -z $PG_HOST $PG_PORT; do
    echo "Wait for DB"
    sleep 1
done

while ! nc -z $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT; do
    echo "Wait for ES"
    sleep 1
done

echo "run etl process.."
python main.py