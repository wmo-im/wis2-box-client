# wis2box-client

Code demonstrating how to download data from a wis2box using pywis-pubsub.

Before starting please replace line 1 of local.yml with connection-string of MQTT-broker.

Build and start client using docker (-d for detached)
```docker build -t wis2-subscriber wis2-subscriber && docker run --name wis2-subscriber --rm -d wis2-subscriber```

... or using docker-compose (-d for detached)
```docker-compose up -d --build```