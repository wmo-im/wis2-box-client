import os
import requests
import json
import base64
import hashlib
import traceback
import time
import logging
import random
import re


from jsonschema import validate

#logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

DEBUG = os.getenv("DEBUG", 'True').lower() in ('true', '1', 't')
ONLY_BUFR = os.getenv("ONLY_BUFR", 'True').lower() in ('true', '1', 't')
TOKEN = os.environ.get('AUTH_TOKEN',None)

log_level = logging.DEBUG if DEBUG else logging.INFO
logging.basicConfig(level=log_level)


url = os.environ.get('MQ_URL', 'amqp://guest:guest@localhost:5672/%2f') # the location and username password of the broker

mq_schema = None

if url.startswith("amqp"):
    import pika
    logging.getLogger("pika").setLevel(logging.WARNING)
    mq_schema="amqp"
elif url.startswith("mqtt"):
    import paho.mqtt.client as mqtt
    logging.getLogger("mqtt").setLevel(logging.WARNING)
    mq_schema="mqtt"
else:
    raise Exception(f"schema of {url} not supported")

routing_key = os.environ.get('ROUTING_KEY',"mw.#") # topic to subscribe to
out_dir = r"./out" # output directory. Subdirectories corresponding to the topic structure will be created, if needed.

# the JSON grammar of the message structure, need a new message-schema as format has changed
#schema = json.load(open("message-schema.json"))

def process_message_content(message):
    """ either download message or obtain it directly from the message structure"""
    if "content" in message:
        content = base64.b64decode(message["content"]["value"])  
    else:
        
        headers = { "Authorization" : "Bearer " + TOKEN } if TOKEN else {}
        # ensure compatibility with old/new format
        if "links" in message:
            data_url = message["links"][0]["href"]
        else: 
            data_url = message["baseUrl"] + message["relPath"]
        resp = requests.get(data_url, headers = headers)
        resp.raise_for_status()
        content = resp.content
        
    return content

def parse_mqp_message(message,topic):
    """ Function that receives a MQP notification based on the WIS 2.0 specifications, as well as the routing key (topic).
    Obtains the file of the notification, either from the message directly or via downloading.
    Checks the file integrity and writes out the file into the output directoy, in a folder hirarchy based on the topic.
    """
    
    message = json.loads(message)

    logging.debug( "MQP message: {}".format(message)) 
    
    validate(instance=message, schema=schema) # check if the message structure is valid
    # we only support base64 encoding and sha512 checksum at this point
    if "content" in message and not message["content"]["encoding"] == "base64":
        raise Exception("message encoding not supported")
    if not message["integrity"]["method"] == "sha512":
        raise Exception("message integrity not supported")
        
    path, filename = os.path.split(message["relPath"])
    
    if ONLY_BUFR and not filename.endswith(".bufr4"):
        return False
        
    content = process_message_content(message)
        
    # check message length and checksum. Only sha512 supported at the moment
    content_hash = base64.b64encode( hashlib.sha512(content).digest() ).decode("utf8")
    if len(content) != message["size"]:
        raise Exception("integrity issue. Message length expected {} got {}".format(len(content),message["size"]))
    if content_hash != message["integrity"]["value"]:
        logging.warning("checksum problem. Check old style encoding")
        if hashlib.sha512(content).hexdigest() != message["integrity"]["value"]:
            raise Exception("integrity issue. Expected checksum {} got {}".format(content_hash,message["integrity"]["value"]))

    topic_dir = os.path.join( out_dir , topic.replace(".","/") )
    
    os.makedirs( topic_dir , exist_ok=True )
    out_file = os.path.join(topic_dir,filename)
    with open( out_file , "wb" ) as fp:
        fp.write(content)
        
    logging.info("Obtained and wrote file: {}".format(out_file))

def callback(ch, method, properties, body):
    """callback function, called when a new notificaton arrives""" 
    topic = method.routing_key
    
    logging.info("Received message with topic: " + topic )
    try:
        parse_mqp_message(body,topic)
    except Exception as e:
        logging.error("exception during mqp processing: {}".format( traceback.format_exc() ))
        
        
def setup_amqp(url):

    while(True):

        try:
            # connect to the broker
            params = pika.URLParameters(url)
            logging.info(" establishing connection to {}".format(params))
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            # create a queue and bind it to the topic defined above. 
            # The queue will get a random name assigned, which is different across invokations of the script. 
            # This means that messages arriving when not connected will not be received. For this a unique name must be given to the queue.
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange="amq.topic", queue=queue_name, routing_key=routing_key)

            # configure callback function
            channel.basic_consume(queue_name,
                                  callback,
                                  auto_ack=True)
            
            try:
                # start waiting for messages
                logging.info(' [*] Waiting for messages:')
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
                break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment this to make the example not attempt recovery
            # from server-initiated connection closure, including
            # when the node is stopped cleanly
            #
            # break
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            logging.error("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            logging.error("Connection was closed, retrying...")
            time.sleep(0.5)
            continue
                       
    # gracefully stop and close ressources
    connection.close()
    
def sub_connect(client, userdata, flags, rc, properties=None):
    logging.info(f"on connection to subscribe: {mqtt.connack_string(rc)}")
    for s in ["xpublic/#"]:
        client.subscribe(s, qos=1)
        
def sub_message_content(client, userdata, msg):
    """
      print messages received.  Exit on count received.
    """
    
    parse_mqp_message(msg.payload.decode('utf-8'),msg.topic)
    
    
    
def parse_connection_string(url):
    # amqps://leesvecc:6af6XRjOxbINH89YTCoD9wz8f2LDT_hZ@cow.rmq2.cloudamqp.com/leesvecc

    m = re.match("(?P<schema>\w+)://(?P<user>\w+):(?P<pass>\w+)@(?P<host>[.\w]+):(?P<port>\d+)", url)
    
    config = m.groupdict()
    
    #logging.debug(f"url match {config}")
    
    return config
    
def setup_mqtt(url):

        config = parse_connection_string(url)

        r = random.Random()
        clientId = f"{__name__}_{r.randint(1,1000):04d}"

        client = mqtt.Client(client_id=clientId, protocol=mqtt.MQTTv5)
        client.on_connect = sub_connect
        client.on_message = sub_message_content
        client.username_pw_set(config["user"], config["pass"])
        client.connect(config["host"])

        # this reconnects, no need to handle disconnects
        client.loop_forever()
           
        

def main():

    logging.info("  [+] setting up MQP consumer")
    logging.debug(f"url {url} schema: {mq_schema}")
    
    if mq_schema == "amqp":
        setup_amqp(url)
    elif mq_schema == "mqtt":
        setup_mqtt(url)

if __name__ == "__main__":
    main()
