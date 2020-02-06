# Imports
import paho.mqtt.subscribe as subscribe
import Adafruit_DHT, bluetooth, logging, sys, threading
from queue import Queue
from datetime import datetime
from r7insight import R7InsightHandler
from time import sleep

# Communication Protocol
REQUEST_TO_SWITCH_TO_BLUETOOTH = 1
REQUEST_TO_SWITCH_TO_MQTT = 2
REQUEST_TO_TERMINATE = -1

# Hardware Settings
sensor = 11 # DHT-11
pin = 4 # GPIO-4

# Device Settings
sensorID = "Gateway"
# LogEntriesToken = "0e3f12ec-eb2e-4dfb-b664-38d4766af33f" # Part 1
# LogEntriesToken = "64d57402-d47b-4ec1-8235-a2c8b41a0dd3" # Part2
LogEntriesToken = "b84e2a88-c8c0-47d4-b441-5db8d86a557e" # temp

# Runtime Variables
isModeMQTT = True
isAlive = True

# Bluetooth Variables
port = 2
client_sock, server_sock = None, None

# MQTT settings
broker_address = "127.0.0.1"
topic = "sensor/data"
qualityOfService = 2
portNumber = 1883

# LogEntries Functions
def setupLogEntries():
    global log
    log = logging.getLogger('r7insight')
    log.setLevel(logging.INFO)
    test = R7InsightHandler(LogEntriesToken, 'eu')

    log.addHandler(test)

def logToLogEntries(msg):
    log.info(msg)
    print("LogEntries: "+msg)

# Bluetooth Functions
def setupBluetoothServer():
    global server_sock
    server_sock = bluetooth.BluetoothSocket( bluetooth.RFCOMM )
    server_sock.bind(("",port))
    server_sock.listen(1)
    print("Server started at Port {}".format(port))

def acceptConnection():
    global client_sock
    client_sock, address = server_sock.accept()
    print("Accepted connection from ", address)

def getDataViaBluetooth():
    data = client_sock.recv(1024)
    print("\tRFCOMM: Received [%s]" % data)
    return data.decode('UTF-8')

# MqttSubscriber class
def setupMqttSubscriber():
    global mqttSubscriber
    mqttSubscriber = MqttSubscriber()
    mqttSubscriber.subscribe()

class MqttSubscriber():
    def __init__(self):
        self.msgQueue = Queue()
        self.thread = threading.Thread(target=self.setup)
        self.thread.daemon = True
        self.active = True

    def subscribe(self):
        self.thread.start()

    def unsubscribe(self):
        self.active = False

    def setup(self):      # On a different thread
        def subscriberCallback(client, userdata, message):
            if(not self.active):
                sys.exit()      # Exit thread
            # print("\tsubscriberCallback: %s : %s" % (message.topic, message.payload.decode()))
            self.msgQueue.put(message.payload.decode())
        
        subscribe.callback(subscriberCallback, topic, hostname=broker_address, port=portNumber, qos=qualityOfService)
    
    def getMqttMessage(self, wait_time=2, interval=0.1):
        if not self.msgQueue.empty():   # Check if queue has data
            return self.msgQueue.get()
        for _ in range(int(wait_time/interval)): # waits 'wait_time' seconds checking at 'interval' second intervals
            sleep(interval)
            if not self.msgQueue.empty():
                return self.msgQueue.get()
        return None


# Utils
def getLogString(temperature, humidity):
    t = datetime.now()
    t = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    string = t + "    SensorID={}, Temperature={}, Humidity={}".format(sensorID,float(temperature), int(humidity))
    return(string)

def closeConnections():
    print("Trying to close connections")
    try:
        if client_sock is not None:
            client_sock.close()
    except Exception as e:
        print(e)
    try:
        if server_sock is not None:
            server_sock.close()
    except Exception as e:
        print(e)

def getDataFromEdgeNode():
    global data
    if isModeMQTT:
        data = mqttSubscriber.getMqttMessage(wait_time=4)
        print("\tMQTT: Received [%s]" % data)
    else:
        data = getDataViaBluetooth()
    if data is not None:
        serviceRequests(data)
    return data

def serviceRequests(data):
    global isAlive, isModeMQTT
    req = int(data.split("Req=")[-1])
    # print("\treq = {}".format(req))
    if req == 0:
        return
    if req == REQUEST_TO_SWITCH_TO_BLUETOOTH and isModeMQTT:
        print("\t\t\tREQUEST_TO_SWITCH_TO_BLUETOOTH")
        isModeMQTT = False
    elif req == REQUEST_TO_SWITCH_TO_MQTT and not isModeMQTT:
        print("\t\t\tREQUEST_TO_SWITCH_TO_MQTT")
        isModeMQTT = True
    elif req == REQUEST_TO_TERMINATE:
        print("\t\t\tREQUEST_TO_TERMINATE")
        isAlive = False

def main():
    setupLogEntries()
    setupBluetoothServer()
    acceptConnection()
    setupMqttSubscriber()
    sleep(5)
    print("SETUP Complete")

    while(isAlive):
        # Get Data from edgeNode
        clientLogString = getDataFromEdgeNode()

        # Read Data from sensor
        humidity, temperature = Adafruit_DHT.read_retry(sensor, pin)

        # Log Data
        logString = getLogString(temperature, humidity)
        if clientLogString is not None:
            logToLogEntries(clientLogString)
        logToLogEntries(logString)
        
        # Wait for 5 seconds
        sleep(5)
    closeConnections()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        closeConnections()
        raise e
