#!/bin/bash
sudo docker run -it --link copy-redis:redis --rm redis redis-cli -h redis -p 6390
