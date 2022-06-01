# wis2-box-client
A client to use with the wis2-box

Testing of the client
```docker build -t wis2-box-client:latest . && docker run --rm -it --env DEBUG=True --env ROUTING_KEY=test --env CLOUDAMQP_URL=SECRET --env AUTH_TOKEN=SUPER-SECRET -v %cd%\out:/app/out wis2-box-client:latest```
