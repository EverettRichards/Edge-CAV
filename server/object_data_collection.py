# parking_broker.py
import paho.mqtt.client as mqtt
import json
from collections import defaultdict as dd
import time
import numpy as np
from colors import *
from server_config import config as settings
from time import sleep as wait
import argparse
import os

if not os.path.exists('outputs'):
   os.makedirs('outputs')
if not os.path.exists('outputs/objects'):
   os.makedirs('outputs/objects')

broker_IP = "localhost"
port_Num = 1883
last_verdict_time = 0.0

broker_start_time = 0

client_config_file = open("object_config.json","r")
client_config_str = client_config_file.read()
client_config_data = json.loads(client_config_str)
client_config_file.close()

object_locations = client_config_data["object_locations"]
vehicle_locations = client_config_data["vehicle_locations"]

NoneObject = ["None",0.1,0.0]

object_history = [] # Contents look like: 0.75, 0.67, ... THIS is a list of OBJECT decisions based on snapshot accuracy %
verdict_id = 0

parser = argparse.ArgumentParser(description="Consolidated Broker for Object Detection Data")
parser.add_argument("-id",type=int,help="Test ID number",default=0)
args = parser.parse_args()
test_id = args.id

def log_decision(verdicts):
    # Objects
    accuracy = len([v for i,v in verdicts["objects"].items() if object_locations[i]==v]) / len(verdicts["objects"])
    object_history.append(accuracy)
    # Clear the oldest piece of data if the log is too long
    if len(object_history) > client_config_data["max_decision_history"]:
        object_history.pop(0)


def print_decision_report():
    global broker_start_time
    print()
    # Print the accuracy of all available decisions, for both QR plate detection and object detection
    print(f"Mean OBJECT accuracy in last {getYellow(len(object_history))} verdicts: {getGreen(np.round(np.mean(object_history)*100,3))}%")
    # Determine how far along we are in the experiment
    ratio = (len(object_history)-10)/(client_config_data['max_decision_history']-10)*50
    avg_time_per_verdict = (time.time()-broker_start_time) / len(plate_history)
    if avg_time_per_verdict < 0.1 or avg_time_per_verdict > 2:
        avg_time_per_verdict = 1
    # Progress / status bar
    print(f"[{getCyan('#'*int(ratio))}{'.'*(50-int(ratio))}]")
    print(f"Progress: {getYellow(verdict_id-10)}/{client_config_data['max_decision_history']} ({getGreen(np.round((verdict_id-10)/client_config_data['max_decision_history']*100,3))}%). ETA: {getYellow(np.round((client_config_data['max_decision_history']-verdict_id+10)*avg_time_per_verdict,3))}s")

class Client:
    def __init__(self,client_name):
        self.name = client_name
        self.decision = None
        self.reputation = 0.5
        self.object_history = []

    def makeDecision(self,decision):
        self.decision = decision

    def getDecision(self):
        return self.decision
    
    def setDecision(self,decision):
        self.decision = decision
    
    def getReputation(self):
        return self.reputation
    
    def getObjectHistory(self):
        return self.object_history
    
    def getAccuracyReport(self):
        line2 = f"Accuracy of last {getYellow(len(self.object_history))} OBJECT votes: {getGreen(np.round(np.mean(self.object_history)*100,3))}%"
        return (line1+'\n'+line2) if len(self.plate_history) > 0 else "No decisions made yet."
    
    def noteOutcome(self,verdicts):
        val = 0
        # Update accuracy history...
        dec = self.decision
        if dec != None:
            val = 0
            obj_val = 0
            for id,obj in dec["object_list"].items():
                if obj == None or len(obj)==0:
                    continue
                my_dec = max(obj,key=obj.get)
                if my_dec != None and my_dec != "None" and verdicts["objects"][id] == my_dec:
                    obj_val += 1
            self.object_history.append(obj_val / len(verdicts["objects"]))
            # Trim the decision history to prevent memory leakage
            if len(self.plate_history) > client_config_data["max_decision_history"]:
                self.plate_history.pop(0)
                self.object_history.pop(0)

        # Update reputation...
        try:
            # Don't do anything if you made NO decisions
            if self.getDecision() == None:
                return
            #self.reputation = clamp(self.reputation + sum(comparisons) * settings["reputation_increment"], settings["min_reputation"], 1)
            dec = self.getDecision()['parking_list']
            # Return the number of decisions that were changed (disagreements)
            val = len(empty_locations) - len(dec)
        except Exception as e:
            print(e)
        finally:
            return val

    def getName(self):
        return self.name

    def __str__(self):
        return self.name + ": " + str(self.decision)

    def __repr__(self):
        return self.name + ": " + str(self.decision)


main_client = None

def clamp(value,min_value=0.0,max_value=1.0):
    return max(min_value, min(value, max_value))

def encodePayload(data):
    data["source"] = "main_broker"
    output = bytearray()
    output.extend(map(ord,json.dumps(data)))
    return output

def decodePayload(string_data):
    return json.loads(string_data)

def publish(CLIENT,topic,message):
    CLIENT.publish(topic,payload=encodePayload(message),qos=0,retain=False)

def on_connect(CLIENT, userdata, flags, rc):
    prCyan(f"Connected with result code {rc}")
    # Subscribe to view incoming client messages
    CLIENT.subscribe("new_client")
    CLIENT.subscribe("end_client")
    # Subscribe to view incoming data from clients
    CLIENT.subscribe("data_V2B")
    CLIENT.subscribe("request_config")

activeClients = []

def issueConfig():
    CLIENT.publish("config",payload=client_config_str,qos=0,retain=False)

def initializeClient(client_name):
    try:
        for client in activeClients:
            if client.name == client_name:
                raise Exception("Client already exists")
        new_client = Client(client_name)
        activeClients.append(new_client)
        activeClients.sort(key=lambda x: x.name)
        issueConfig()
        prCyan("Added client: "+client_name)
        return new_client
    except:
        prRed("Failed to add client. Client already exists: "+client_name)

def removeClient(client_name):
    try:
        for client in activeClients:
            if client.name == client_name:
                activeClients.remove(client)
                prCyan("Removed client: "+client_name)
                return
        raise Exception("Client not found")
    except:
        prRed("Failed to remove client. Client not found: "+client_name)

def getClosestObject(parking_list,pos):
    closest_id = 0
    closest_distance = -1
    for i,obj in enumerate(parking_list):
        distance = np.sqrt((obj['x']-pos['x'])**2 + (obj['y']-pos['y'])**2)
        if distance < closest_distance or closest_distance == -1:
            closest_distance = distance
            closest_id = i
    return closest_id

def getDistance(x1,y1,x2,y2):
    return np.sqrt((x1-x2)**2 + (y1-y2)**2)

final_outputs = {}

def quitIfExhausted():
    global verdict_id
    global final_outputs
    if verdict_id >= client_config_data["max_decision_history"] + 10 or verdict_id<0:
        if verdict_id > 0:
            # Tell the clients that the data collection is done. Communication is key! :)
            publish(main_client,"finished",{"message":"I'm done!"})
            # Display the config data:
            print(f"\nConfig data: {getCyan(client_config_data)}")
            pkg = {
                "object_locations":object_locations,
                "vehicle_locations":vehicle_locations,
                "raw_data":final_outputs,
                "config":client_config_data,
                "test_id":test_id
            }
            output_file = open(f"outputs/objects/output_{test_id}.json","w")
            output_file.write(json.dumps(pkg,indent=4))
            wait(1)
            exit(0)
        verdict_id = -1
        return True
    else:
        return False

def getVerdict():
    global last_verdict_time
    global verdict_id

    # Exit out of the loop after all the necessary data has been compiled!
    if quitIfExhausted(): return

    NOW = time.time()
    
    # Refresh the last verdict time
    last_verdict_time = NOW
    verdict_id += 1 # Increment the verdict ID

    # Clear the output log
    print("\033[H\033[J", end="")

    if verdict_id <= 10:
        print("-"*40)
        print(f"Waiting for verdicts to accumulate ({getCyan(verdict_id)}/10)...")
        print("-"*40)
        return
    else:
        print("-"*40)
        max_dec = client_config_data["max_decision_history"]
        print(f"Getting verdict #{getYellow(verdict_id-10)}/{max_dec} ({np.round((verdict_id-10)/max_dec,0)}%) (t=...{getCyan(np.round(NOW%10000,3))}s)")
        print("-"*40)
    
    # Initialize a list of blank Default Dictionaries to count occurrences of each decision
    global dd

    for client in activeClients:
        #local_weight_factor = 1 # This variable will serve as the reliability of the vehicle
        decision = client.getDecision()
        # Throw out expired decisions
        if decision == None or decision["timestamp"] < NOW - settings["oldest_allowable_data"]:
            print(f"Skipping client: {client.getName()}")
            continue
        ############################################################################################################
        ''' DO OBJECT DETECTION STUFF '''
        ############################################################################################################
        name = client.getName()
        if name in final_outputs.keys():
            final_outputs[name].append(decision["object_list"])
        else:
            final_outputs[name] = [decision["object_list"]]
    

    

def didEveryoneDecide():
    for client in activeClients:
        if client.getDecision() == None:
            return False
    return True

def getClientByName(client_name):
    for client in activeClients:
        if client.getName() == client_name:
            return client
    return None

def interpretData(payload):
    client = getClientByName(payload["source"])
    payload["timestamp"] = time.time()
    if client == None:
        prCyan("Attempting to create new client, "+payload["source"])
        client = initializeClient(payload["source"])
        if client == None:
            prRed("Failed to create new client")
            return
    client.setDecision(payload)
    if time.time() - last_verdict_time > settings["verdict_min_refresh_time"]:
        getVerdict()

# The callback function, it will be triggered when receiving messages
def on_message(CLIENT, userdata, msg):
    global broker_start_time
    if broker_start_time == 0:
        broker_start_time = time.time()
    # Turn from byte array to string text
    payload = msg.payload.decode("utf-8")
    # Turn from string text to data structure
    payload = decodePayload(payload)
    # Decide what to do, based on the message's topic
    if msg.topic == "new_client":
        # Add a new client!
        initializeClient(payload["source"])
    elif msg.topic == "end_client":
        # Remove an existing client. Sad!
        removeClient(payload["source"])
    elif msg.topic == "data_V2B":
        # Interpret the data
        interpretData(payload)
    elif msg.topic == "request_config":
        issueConfig()

CLIENT = mqtt.Client()
CLIENT.on_connect = on_connect
CLIENT.on_message = on_message
main_client = CLIENT

# Set the will message, when the Raspberry Pi is powered off, or the network is interrupted abnormally, it will send the will message to other clients
CLIENT.will_set('finished', encodePayload({"message":"I'm offline"}), qos=0, retain=False)

# Create connection, the three parameters are broker address, broker port number, and keep-alive time respectively
CLIENT.connect(broker_IP, port_Num, keepalive=60)

# Set the network loop blocking, it will not actively end the program before calling disconnect() or the program crash
CLIENT.loop_forever()