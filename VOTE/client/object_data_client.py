# vehicle.py
# TEST CHANGE 4
import paho.mqtt.client as mqtt
import json
from network_config import broker_IP, port_num
from robot_hat import TTS
import socket
import time
from time import sleep as wait
from vilib import Vilib # Built-in SunFounder computer vision library
from picarx import Picarx
from multiprocessing import Process # Concurrency library, since we have 2 infinite loops going on here...
import numpy as np
from collections import defaultdict as dd
import os
from colors import *

config = None

client_name = socket.getfqdn()

def deleteLocalConfig():
    try:
        os.remove("config.json")
    except:
        pass

def encodePayload(data):
    data["source"] = client_name
    output = bytearray()
    output.extend(map(ord,json.dumps(data)))
    return output

def decodePayload(string_data):
    return json.loads(string_data)

def publish(client,topic,message):
    client.publish(topic,payload=encodePayload(message),qos=0,retain=False)
    print(f"{getYellow('Emitted message')} (t=...{getCyan(round(time.time()%10000,3))}s",end="\r")

def on_connect(client, userdata, flags, rc):
    prCyan(f"Connected with result code {rc}")
    # Subscribe to view incoming verdicts
    client.subscribe("verdict")
    client.subscribe("msg_B2V")
    client.subscribe("config")
    client.subscribe("finished")
    # Tell the server that this client exists! Add it to the registry.
    publish(client,"new_client",{"message":"New Client :)"})

def processVerdict(payload):
    prYellow(f"Verdict received. The objects are: " + str(payload["message"]))

def writeConfig(payload):
    global config
    if config != None: return
    config = payload
    conf_file = open("config.json","w")
    conf_file.write(json.dumps(config))
    prCyan(f"Configuration data received!")

def waitForConfig():
    global config
    while config == None:
        try:
            publish(client,"request_config",{"message":"Please send me the config!"})
            conf_file = open("config.json","r")
            config = json.loads(conf_file.read())
        except:
            prRed("Config not received yet. Waiting...")
        wait(1.0)

# The callback function, it will be triggered when receiving messages
def on_message(client, userdata, msg):
    global px
    # Turn from byte array to string text
    payload = msg.payload.decode("utf-8")
    # Turn from string text to data structure
    payload = decodePayload(payload)
    # Handle the message
    if msg.topic == "verdict":
        # Receive a verdict from the server. Utilize it.
        processVerdict(payload)
    elif msg.topic == "config":
        writeConfig(payload)
    elif msg.topic == "finished":
        prGreen("The server has finished its task. Have a nice day!")
        tts = TTS()
        tts.say("Data collection is complete. Please shut down the vehicle or restart the program.")
        exit(0)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Set the will message, when the Raspberry Pi is powered off, or the network is interrupted abnormally, it will send the will message to other clients
client.will_set('end_client',encodePayload({"message":"Client Expired :("}))

# Create connection, the three parameters are broker address, broker port number, and keep-alive time respectively
client.connect(broker_IP, port_num, keepalive=60)

# Set the network loop blocking, it will not actively end the program before calling disconnect() or the program crash
def network_loop():
    client.loop_forever()

def find_closest_object(dict_of_numbers, number):
    return min(dict_of_numbers.keys(), key=lambda x:abs((dict_of_numbers[x])-number))

def get_distance(obj,car):
    obj_loc = config["object_locations"][obj]
    car_loc = config["vehicle_locations"][car]
    return np.sqrt((obj_loc["x"]-car_loc["x"])**2 + (obj_loc["y"]-car_loc["y"])**2)

def distance(x1,y1,x2,y2):
    return np.sqrt((x1-x2)**2 + (y1-y2)**2)

horizontal_angle_per_pixel = None
screen_center_x = None

def get_angular_width(x1,x2): # Takes value in PIXELS
    global horizontal_angle_per_pixel, screen_center_x
    delta_x = x2 - x1
    delta_theta = delta_x * horizontal_angle_per_pixel
    return delta_theta

def get_angle_of_detected_thing(y1,x1,y2,x2):
    global horizontal_angle_per_pixel, screen_center_x
    if x2-x1<1: # Only scale if not scaled already.
        y1,y2 = y1*config["image_height"],y2*config["image_height"] # Adjust to pixel size
        x1,x2 = x1*config["image_width"],x2*config["image_width"] # Adjust to pixel size
    # Find the center point (px) of the object
    x_center = (x1+x2)/2
    # Find out how many degrees off-center the detected object is
    delta_x = x_center - screen_center_x
    delta_theta = delta_x * horizontal_angle_per_pixel
    return delta_theta

def get_angle_to_object(obj):
    # Calculate on-screen angle between object and robot, using label
    bounds = obj["bounding_box"]
    y1,x1,y2,x2 = bounds # IDK what order these are actually presented in. CALIBRATE!
    return get_angle_of_detected_thing(y1,x1,y2,x2)

def get_angle_to_qr(qr):
    return get_angle_of_detected_thing(qr["y"],qr["x"],qr["y"]+qr["h"],qr["x"]+qr["w"])

def rad(deg):
    return deg * np.pi / 180

def StartCamera():
    Vilib.camera_start(vflip=False, hflip=False)
    Vilib.show_fps()
    Vilib.display(local=True, web=True)
    #wait(1)
    Vilib.object_detect_switch(config["detect_objects"]) # Enable object detection
    Vilib.qrcode_detect_switch(config["detect_plates"]) # Enable QR detection

px = None
tts = None

# This function gradually moves the camera pan to a specific angle, using the Picarx library.
def moveCameraToAngle(px,angle):
    mult = 1 if angle>0 else -1
    for i in range(abs(angle)):
        px.set_cam_pan_angle(i*mult)
        if i%3==0:
            wait(0.03)
    px.set_cam_pan_angle(angle)

horizontal_angle_per_pixel = None
vertical_angle_per_pixel = None
qr_code_size_inches = None
current_vehicle_orientation = None
offset_angle = 0
offset_angle_direction = 1

# This function calculates the distance and position of each QR code in the list, relative to the robot's current position and orientation.
def getRevisedQrList(qr_list,current_vehicle_location):
    global horizontal_angle_per_pixel
    global vertical_angle_per_pixel
    global qr_code_size_inches
    global current_vehicle_orientation
    qr_list = sorted(qr_list,key=lambda qr: qr['x'])
    for i,qr in enumerate(qr_list):
        new_qr = {'text':qr['text']}
        angle_from_center = get_angle_to_qr(qr)
        # Figure out how many degrees the QR code spans
        angular_width = qr['w'] * horizontal_angle_per_pixel
        angular_height = qr['h'] * vertical_angle_per_pixel
        angular_avg_size = np.sqrt(angular_width*angular_height)
        # Calculate the object's distance from camera using basic trig + knowledge of fixed QR code size
        distance_from_camera = qr_code_size_inches / (2 * np.tan(rad(angular_avg_size)/2))
        new_qr['distance'] = distance_from_camera
        # The objective orientation of the detected QR code, relative to central axis
        focus_orientation = current_vehicle_orientation - angle_from_center

        new_qr['position'] = {
            'x':current_vehicle_location['x'] + distance_from_camera * np.cos(rad(focus_orientation)),
            'y':current_vehicle_location['y'] + distance_from_camera * np.sin(rad(focus_orientation)),
        }
        qr_list[i] = new_qr
    return qr_list

# This function prints out the list of QR codes that the robot has seen. It will print out the text of the QR code, its global position, and its distance from the robot.
def printQrList(qr_list):
    prCyan("-"*40)
    for i,qr in enumerate(qr_list):
        main_word = None
        if qr['text'] == "EMPTY":
            main_word = getRed(qr['text']) + "  "
        else:
            main_word = getGreen(qr['text'])
        output = f"Parking Spot {i} is: {main_word} GLOBAL POSITION: ({qr['position']['x']:.2f},{qr['position']['y']:.2f}). Dist={qr['distance']:.2f}in."
        print(output)
    prCyan("-"*40)

# This function updates the list of QR codes that the robot has seen. It will add new QR codes, update existing ones, and remove ones that are no longer visible.
def updateQrList(qr_list,found_plates):
    for qr in qr_list:
            if qr['text'] == "EMPTY": # If the QR code is empty, we need to check if it's close to another empty QR code
                for qr2 in found_plates:
                    if qr2['text'] == "EMPTY" and distance(qr['position']['x'],qr['position']['y'],qr2['position']['x'],qr2['position']['y']) < 2:
                        found_plates.remove(qr2)
                        break
                found_plates.append(qr)
            else: # If the QR code is not empty, we need to check if it's already in the list. If it is, we need to update it. If not, we need to add it.
                for qr2 in found_plates:
                    if qr2['text'] == qr['text']:
                        found_plates.remove(qr2)
                        break
                found_plates.append(qr)

angles_to_each_object = None

def processDetectedObject(obj):
    # Get the bounding box parameters from Vilib
    y1,x1,y2,x2 = obj["bounding_box"]
    # Scale the boundaries from ratio (0.000 to 1.000) to pixel size (0 to 480/640)
    y1,y2 = y1*config["image_height"],y2*config["image_height"] # Adjust to pixel size
    x1,x2 = x1*config["image_width"],x2*config["image_width"] # Adjust to pixel size
    y1,x1,y2,x2 = int(y1),int(x1),int(y2),int(x2)

    x_center = (x1+x2)/2

    # Find out how many degrees off-center the detected object is
    delta_x = x_center - screen_center_x
    delta_theta = delta_x * horizontal_angle_per_pixel

    # Determine which of the known objects this object is closest to, in terms of angle
    closest_object = find_closest_object(angles_to_each_object,delta_theta)
    closest_angle = angles_to_each_object[closest_object]
    angle_difference = abs(delta_theta - closest_angle)
    return closest_object,angle_difference

def convert_to_serializable(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.float32):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(i) for i in obj]
    else:
        return obj

# This is the main loop of the program. It will run forever, updating the list of QR codes that the robot sees and sending it to the server.
def MainLoop():
    global px
    global config
    global current_vehicle_orientation
    global current_vehicle_location
    global horizontal_angle_per_pixel
    global vertical_angle_per_pixel
    global qr_code_size_inches
    global horizontal_angle_per_pixel
    global screen_center_x
    global angles_to_each_object
    global offset_angle
    global offset_angle_direction
    global dd

    px = Picarx()
    px.set_cam_pan_angle(0)
    px.set_cam_tilt_angle(-10)
    waitForConfig()
    StartCamera()
    wait(1)

    # Import from the collective config file
    vehicle_locations = config["vehicle_locations"]
    object_locations = config["object_locations"]
    default_location = vehicle_locations[client_name]

    # The current car's physical location in 2D space
    initial_vehicle_location = {
        'x': default_location["x"],
        'y': default_location["y"],
    }
    current_vehicle_location = initial_vehicle_location

    initial_vehicle_orientation = (vehicle_locations[client_name]["car_angle"] - vehicle_locations[client_name]["camera_angle"]) % 360
    moveCameraToAngle(px,vehicle_locations[client_name]["camera_angle"])
    current_vehicle_orientation = initial_vehicle_orientation

    angles_to_each_object = {} # Strictly in Degrees. The expected angles from the robot to each of the listed object locations.

    # Initialize the angles to each object
    for obj in object_locations.keys():
        obj_loc = object_locations[obj]
        theta = np.arctan2(obj_loc["y"]-current_vehicle_location["y"],obj_loc["x"]-current_vehicle_location["x"]).item()
        angles_to_each_object[obj] = (current_vehicle_orientation - (theta * 180 / np.pi)) % 360

    # Calculate some basic constants based on the configuration
    horizontal_angle_per_pixel = config["horizontal_FOV"] / config["image_width"]
    vertical_angle_per_pixel = config["vertical_FOV"] / config["image_height"]
    screen_center_x = config["image_width"] / 2

    qr_code_size_inches = 1 + 15/16 # Constant width of QR codes

    last_published = time.time()
    found_objects = {} # Where longitudinal non-license plate objects will be stored

    for id in object_locations.keys():
        found_objects[id] = dd(float) # Each is a dd with index=label and value=score
        '''
        Looks like:
        {
        "cup": {'cup': 0.0, 'bottle': 90.0, 'person': 7.0, 'laptop': 0.0, 'cell phone': 0.0},
        "bottle": {'cup': 100.0, 'bottle': 0.0, 'person': 0.0, 'laptop': 0.0, 'cell phone': 0.0},
        "person": {'bottle': 60.0, 'cell phone': 0.0},
        ...
        }
        '''

    local_iteration_count = 0

    detected_objects = None

    # Continue forever, updating the QR list every time and sending it to the server occasionally
    while True:
        local_iteration_count += 1
        ############################################################################################################
        ''' DO OBJECT DETECTION STUFF '''
        ############################################################################################################
        if config["detect_objects"]:
            # Acquire list of detected objects
            detected_objects = Vilib.object_detection_list_parameter.copy()

            for obj in detected_objects:
                if not "class_name" in obj.keys() or not "score" in obj.keys():
                    continue
                closest_object,angle_difference = processDetectedObject(obj)

                if angle_difference < config["angle_threshold"]:
                    this_dd = found_objects[closest_object]
                    this_dd[obj['class_name']] += obj["score"]
        ############################################################################################################
        ''' DO FINAL DECISION STUFF '''
        ############################################################################################################
        # Send out the final decision of what the robot sees!
        '''if time.time() - last_published > config["submission_interval"]:
            if config["detect_objects"]:
                for this_dd in found_objects.values():
                    for key in this_dd.keys():
                        this_dd[key] /= local_iteration_count # Normalize
            # Note the UTC time of data publication
            last_published = time.time()
            # Publish the QR and object lists to the server
            publish(client,"data_V2B",{
                "object_list":found_objects,
            })
            for object_id,this_dd in found_objects.items():
                this_dd.clear()
            local_iteration_count = 0'''
        # Wait for a "tick" of time before continuing to the next cycle
        #wait(config["capture_interval"])
        publish(client,"data_V2B",{
                "object_list":convert_to_serializable(detected_objects),
            })
        wait(config["submission_interval"])

if __name__ == "__main__":
    try:
        deleteLocalConfig()
        Process(target=network_loop).start()
        wait(0.5)
        #while config == None:
            #publish(client,"request_config",{"message":"Please send me the config!"})
            #wait(0.5)
        MainLoop()
    except KeyboardInterrupt:
        pass
    #except Exception as e:
        #print(f"\033[31mERROR: {e}\033[m")
    finally:
        Vilib.camera_close()
        if px:
            px.set_cam_pan_angle(0)
            px.stop()
