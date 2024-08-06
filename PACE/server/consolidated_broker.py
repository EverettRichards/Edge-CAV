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

broker_IP = "localhost"
port_Num = 1883
last_verdict_time = 0.0

broker_start_time = 0

client_config_file = open("consolidated_config.json","r")
client_config_str = client_config_file.read()
client_config_data = json.loads(client_config_str)
client_config_file.close()

empty_locations = client_config_data["empty_parking_spot_locations"]
occupied_locations = client_config_data["occupied_parking_spot_locations"]
truth_values = client_config_data["true_parking_occupants"]
object_locations = client_config_data["object_locations"]
vehicle_locations = client_config_data["vehicle_locations"]

NoneObject = ["None",0.1,0.0]

plate_history = [] # Contents look like: 0.75, 0.67, ... THIS is a list of PARKING decisions based on snapshot accuracy %
object_history = [] # Contents look like: 0.75, 0.67, ... THIS is a list of OBJECT decisions based on snapshot accuracy %
verdict_id = 0

parser = argparse.ArgumentParser(description="Consolidated Broker for Parking Lot Data")
parser.add_argument("-id",type=int,help="Test ID number",default=0)
args = parser.parse_args()
test_id = args.id

def log_decision(verdicts):
    # Plates
    accuracy = len([v for i,v in verdicts["plates"].items() if truth_values[int(i)]==v]) / len(verdicts["plates"])
    plate_history.append(accuracy)
    # Objects
    accuracy = len([v for i,v in verdicts["objects"].items() if object_locations[i]==v]) / len(verdicts["objects"])
    object_history.append(accuracy)
    # Clear the oldest piece of data if the log is too long
    if len(object_history) > client_config_data["max_decision_history"]:
        object_history.pop(0)
        plate_history.pop(0)


def print_decision_report():
    global broker_start_time
    print()
    # Print the accuracy of all available decisions, for both QR plate detection and object detection
    print(f"Mean QR PLATE accuracy in last {getYellow(len(plate_history))} verdicts: {getGreen(np.round(np.mean(plate_history)*100,3))}%")
    print(f"Mean OBJECT accuracy in last {getYellow(len(object_history))} verdicts: {getGreen(np.round(np.mean(object_history)*100,3))}%")
    # Determine how far along we are in the experiment
    ratio = (len(plate_history)-10)/(client_config_data['max_decision_history']-10)*50
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
        self.plate_history = []
        self.object_history = []

    def makeDecision(self,decision):
        self.decision = decision

    def getDecision(self):
        return self.decision
    
    def setDecision(self,decision):
        self.decision = decision
    
    def getReputation(self):
        return self.reputation
    
    def getAccuracyReport(self):
        line1 = f"Accuracy of last {getYellow(len(self.plate_history))} PLATE votes: {getGreen(np.round(np.mean(self.plate_history)*100,3))}%"
        line2 = f"Accuracy of last {getYellow(len(self.object_history))} OBJECT votes: {getGreen(np.round(np.mean(self.object_history)*100,3))}%"
        return (line1+'\n'+line2) if len(self.plate_history) > 0 else "No decisions made yet."
    
    def noteOutcome(self,verdicts):
        val = 0
        # Update accuracy history...
        dec = self.decision
        if dec != None:
            val = 0
            for obj in dec["parking_list"]:
                if obj == None:
                    continue
                if obj['text'] == "EMPTY":
                    closest_spot = getClosestObject(empty_locations,obj['position'])
                    if verdicts["plates"][str(closest_spot)] == "EMPTY":
                        val += 1
                else:
                    if verdicts["plates"][str(getClosestObject(occupied_locations,obj['position']))] == obj['text']:
                        val += 1
            self.plate_history.append(val / len(verdicts["plates"]))

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

def quitIfExhausted():
    global verdict_id
    if verdict_id >= client_config_data["max_decision_history"] + 10 or verdict_id<0:
        if verdict_id > 0:
            # Tell the clients that the data collection is done. Communication is key! :)
            publish(main_client,"finished",{"message":"I'm done!"})
            # Display the config data:
            print(f"\nConfig data: {getCyan(client_config_data)}")
            output_file = open(f"outputs/output_{test_id}.json","w")
            output_file.write(json.dumps({
                "plate_history":plate_history,
                "object_history":object_history,
                "config":client_config_data,
                "client_reports": {client.getName(): {"plates":client.plate_history,"objects":client.object_history} for client in activeClients}
            }))
            wait(1)
            exit(0)
        verdict_id = -1
        return True
    else:
        return False
    
def parseStack(stack,taken_spots):
    while len(stack) > 0:
        this_plate = stack.pop()
        plate,mean_x,mean_y = this_plate
        closest_spot = None
        closest_dist = None
        for i,spot in enumerate(taken_spots):
            # Distance from the mean position of the license plate to the center of the parking spot
            dist = getDistance(spot['position']['x'],spot['position']['y'],mean_x,mean_y)
            # Only consider spots that would actually make an improvement
            if closest_dist == None or dist < closest_dist:
                if spot['plate'] == None: # If the spot is empty, just take it
                        closest_dist = dist
                        closest_spot = i
                else: # If the spot is taken, only take it if the current plate is closer than the one already there
                    if dist < getDistance(spot['position']['x'],spot['position']['y'],spot['plate'][1],spot['plate'][2]):
                        closest_dist = dist
                        closest_spot = i

        closest = taken_spots[closest_spot]

        # If replacing an old item, put it back into the stack
        if closest['plate'] != None:
            stack.append(closest['plate'])

        closest['plate'] = this_plate

def getVerdict():
    global last_verdict_time
    global verdict_id

    # Exit out of the loop after all the necessary data has been compiled!
    if quitIfExhausted(): return

    NOW = time.time()
    '''if (NOW - last_verdict_time) < settings["verdict_min_refresh_time"]:
        print(f"Returning. Now: {NOW}, Last: {last_verdict_time}")
        return''' # This was to debug the issue of the verdicts being too infrequent
    
    # Refresh the last verdict time
    last_verdict_time = NOW
    verdict_id += 1 # Increment the verdict ID

    # Initialize a list of blank Default Dictionaries to count occurrences of each decision
    global dd
    plate_counts = dd(int)
    license_plates = ["EMPTY"] * len(empty_locations)
    # For license plates...
    position_tally = {}

    # Initialize a dictionary of object identities
    object_identities = {}
    for key in object_locations.keys():
        object_identities[key] = dd(float)

    # Clear the output log
    print("\033[H\033[J", end="")

    # Display separator for verdict presentation
    if settings["show_verbose_output"]:
        print("-"*40)
        print(f"Getting verdict #{getYellow(verdict_id)} (t=...{getCyan(np.round(NOW%10000,3))}s)")
        print("-"*40)

    for client in activeClients:
        local_weight_factor = 1 # This variable will serve as the reliability of the vehicle
        decision = client.getDecision()
        # Throw out expired decisions
        if decision == None or decision["timestamp"] < NOW - settings["oldest_allowable_data"]:
            print(f"Skipping client: {client.getName()}")
            continue
        ############################################################################################################
        ''' DO OBJECT DETECTION STUFF '''
        ############################################################################################################
        for object_id,this_dd in decision["object_list"].items():
            if this_dd == None:
                continue
            for key in this_dd.keys():
                object_identities[object_id][key] += this_dd[key] * local_weight_factor
        ############################################################################################################
        ''' DO LICENSE PLATE STUFF '''
        ############################################################################################################
        # Get the list of detected plates
        detected_plates = decision["parking_list"]

        # Go through each detected plate and tally up the position
        for qr in detected_plates:
            if qr['text'] == "EMPTY":
                closest_spot = getClosestObject(empty_locations,qr['position'])
                plate_counts[closest_spot] -= 1
            else:
                if qr['text'] not in position_tally.keys():
                    position_tally[qr['text']] = {'x':0,'y':0,'count':0}

                position_tally[qr['text']]['x'] += qr['position']['x']
                position_tally[qr['text']]['y'] += qr['position']['y']
                position_tally[qr['text']]['count'] += 1

        # Verbose output
        if settings["show_verbose_output"]:
            print(f"@{getPurple(client.getName())} (rep={getYellow(np.round(client.getReputation(),3))}) ({client.getAccuracyReport()}):")
            if len(detected_plates) > 0:
                for qr in detected_plates:
                    print(f"--> {getGreen(qr['text'])} (x={getCyan(np.round(qr['position']['x'],2))},y={getCyan(np.round(qr['position']['y'],2))},|d|={getCyan(np.round(qr['distance'],2))})")
            else:
                print(f"--> {getRed('No QR codes detected')}")
        # example: @euclid (rep=0.500): ABCD123 (x=4.56,y=-6.40, |d|=8.41), IJKL456, XY12ZA3

    print() # Get that nice, sweet newline!
    
    # Record table of average positions for each detected license plate
    stack = []
    taken_spots = [{'position':x,'plate':None} for x in occupied_locations]

    for plate,val in position_tally.items():
        mean_x = val['x'] / val['count']
        mean_y = val['y'] / val['count']
        stack.append([plate,mean_x,mean_y])

    # Optimize the license plate positions into unique 2D spots. Updates the value of taken_spots
    parseStack(stack,taken_spots)
    
    # Empty verdicts table
    plate_verdicts = {}
    object_verdicts = {}
    # Summarize the final plate verdicts in a simpler format
    for i,spot in enumerate(taken_spots):
        if spot['plate'] != None:
            plate,mean_x,mean_y = spot['plate']
            plate_verdicts[str(i)] = plate
        else:
            plate_verdicts[str(i)] = "EMPTY"
    # Update the list of object verdicts
    for key in object_identities.keys():
        object_verdicts[key] = max(object_identities[key],key=object_identities[key].get,default=NoneObject)
    
    verdicts = {
        "plates":plate_verdicts,
        "objects":object_verdicts,
    }

    # Publish the verdict
    publish(main_client,"verdict",{"message":verdicts})

    if settings["show_verbose_output"]:
        for i,spot in enumerate(taken_spots):
            if spot['plate'] != None:
                plate,mean_x,mean_y = spot['plate']
                print(f"{getYellow(i+1)}) Consensus: {getGreen(plate)} ({getCyan(np.round(mean_x,2))},{getCyan(np.round(mean_y,2))})")
            else:
                print(f"{getYellow(i+1)}) Consensus: {getRed('EMPTY')}")
        print()
        for i,obj in object_verdicts.items():
            if obj == None:
                print(f"Object {getYellow(i)}: {getRed('None')}")
            else:
                print(f"Object {getYellow(i)}: {getGreen(obj[0])}")

    # Log the decision
    log_decision(verdicts)

    print_decision_report()

    for client in activeClients:
        client.noteOutcome(verdicts)

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