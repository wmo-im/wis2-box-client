import json
from consume import callback

class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


for file in [r"../test/test-notification-A_ISMC.json",r"../test/test-notification-SNO.json"]:

    body = json.loads(open(file).read())
    method = {"routing_key":"mw.blantyre_chileka.observation.surface.land.automatic.tropics.0-90w"}

    callback({},  Struct(**method) , {} , json.dumps(body)  )