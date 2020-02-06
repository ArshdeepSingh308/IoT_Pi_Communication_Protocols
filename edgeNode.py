# Imports
import paho.mqtt.publish as publish
import Adafruit_DHT, bluetooth, sys
from datetime import datetime
from time import time, sleep

# Communication Protocol
REQUEST_TO_SWITCH_TO_BLUETOOTH = 1
REQUEST_TO_SWITCH_TO_MQTT = 2
REQUEST_TO_TERMINATE = -1

# Hardware Settings
sensor = 11 # DHT-11
pin = 4 # GPIO-4

# Device Settings
sensorID = "Edge"

# Runtime Variables
no_of_days = 3
seconds_per_hour = 10
sleep_time = 5

# Bluetooth Variables
port = 2
serverBluetoothAddress = "B8:27:EB:05:79:FA"
sock = None

# MQTT settings
broker_address = "192.168.137.43"
clientName = sensorID
topic = "sensor/data"
qualityOfService = 2
portNumber = 1883

# Bluetooth Functions
def setupBluetoothClient():
    global sock
    sock = bluetooth.BluetoothSocket(bluetooth.RFCOMM )

def connectBluetooth():
    sock.connect((serverBluetoothAddress, port))
    print("Bluetooth Connection successful")

def sendDataViaBluetooth(msg):
    sock.send(msg)
    print("RFCOMM: "+ msg)

def publishViaMqtt(data):
    publish.single(topic, data, hostname=broker_address, port=portNumber, qos=qualityOfService)
    print("MQTT: "+ data)

# Utils
def getLogString(temperature, humidity, protocol="RFCOMM", req=0):
    t = datetime.now()
    t = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    string = t + "    Protocol={}, SensorID={}, Temperature={}, Humidity={}, Req={}".format(protocol, sensorID,float(temperature), int(humidity), req)
    return(string)

def closeConnections():
    print("Trying to close connections")
    try:
        if sock is not None:
            sock.close()
    except Exception as e:
        print(e)

def switchDayNight(isDay, lastDay=False):
    # Calculate time after which function should switch
    switch_time = time() + (12 * seconds_per_hour) - sleep_time

    while(time() < switch_time):
        readAndLog(isDay, 0)
        sleep(sleep_time)
    # Compute the next request to be made
    nextReq = REQUEST_TO_SWITCH_TO_BLUETOOTH if isDay else REQUEST_TO_SWITCH_TO_MQTT if not lastDay else REQUEST_TO_TERMINATE
    readAndLog(isDay, nextReq)
    sleep(sleep_time)

def readAndLog(isMqtt, nextReq):
    # Read Data
    humidity, temperature = Adafruit_DHT.read_retry(sensor, pin)
    
    # Send Data
    if isMqtt:
        publishViaMqtt(getLogString(temperature, humidity, protocol="MQTT", req=nextReq))
    else:
        sendDataViaBluetooth(getLogString(temperature, humidity, protocol="RFCOMM", req=nextReq))

def main():
    setupBluetoothClient()
    connectBluetooth()
    sleep(5)
    for i in range(no_of_days):
        # Send Data for 12 hours of day (synthetic)
        switchDayNight(True)

        # Send Data for 12 hours of night (synthetic)
        switchDayNight(False, i == no_of_days-1)
    closeConnections()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        closeConnections()
        raise e
        