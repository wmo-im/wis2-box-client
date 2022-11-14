# wis2box-client

Generic client to subscribe to wis2box.

## How to use

Edit local.yml and replace WIS2BOX_BROKER_PUBLIC with the value for WIS2BOX_BROKER_PUBLIC defined for the wis2box you wish to subscribe to.
Optionally update the topic.

Build and start client using docker (-d for detached)
```docker build -t wis2-subscriber wis2-subscriber && docker run --name wis2-subscriber --rm -d wis2-subscriber```

... or using docker-compose (-d for detached)
```docker-compose up -d --build```