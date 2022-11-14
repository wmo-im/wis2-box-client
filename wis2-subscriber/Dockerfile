FROM python:3.8
RUN pip install pywis-pubsub
COPY local.yml /app/
WORKDIR /app
CMD ["pywis-pubsub", "subscribe", "--config", "local.yml","--verbosity","DEBUG"]