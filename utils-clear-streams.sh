#!/bin/bash

topics_regex=$(echo ".*?KSTREAM.*" ".*?KTABLE.*" $@ | sed -e 's/ /\\|/g')
echo "Deleting topics using regex $topics_regex"
./utils-delete-topic.sh "$topics_regex"
