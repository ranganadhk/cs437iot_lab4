# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import datetime
import numpy as np
from threading import Lock 


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0 
device_end = 5

#Path to the dataset, modify this
data_path = "../DataFromClass/UC-Emission_Short.csv"
#data_path = "data/class_{}.csv"

#data_csv = pd.read_csv (r'../DataFromClass/UC-Emission_Short.csv')
data_csv = pd.read_csv (r'../DataFromClass/data2/vehicle0.csv')
data_csv.to_json (r'../DataFromClass/data2/vehicle0.json')
data_json = pd.read_json(r'../DataFromClass/data2/vehicle0.json')
data_json = data_json[['vehicle_CO2','vehicle_speed']]
data_json_vehicle0 = data_json.to_json()


data_csv = pd.read_csv (r'../DataFromClass/data2/vehicle1.csv')
data_csv.to_json (r'../DataFromClass/data2/vehicle1.json')
data_json = pd.read_json(r'../DataFromClass/data2/vehicle1.json')
data_json = data_json[['vehicle_CO2','vehicle_speed']]
data_json_vehicle1 = data_json.to_json()

data_csv = pd.read_csv (r'../DataFromClass/data2/vehicle2.csv')
data_csv.to_json (r'../DataFromClass/data2/vehicle2.json')
data_json = pd.read_json(r'../DataFromClass/data2/vehicle2.json')
data_json = data_json[['vehicle_CO2','vehicle_speed']]
data_json_vehicle2 = data_json.to_json()

data_csv = pd.read_csv (r'../DataFromClass/data2/vehicle3.csv')
data_csv.to_json (r'../DataFromClass/data2/vehicle3.json')
data_json = pd.read_json(r'../DataFromClass/data2/vehicle3.json')
data_json = data_json[['vehicle_CO2','vehicle_speed']]
data_json_vehicle3 = data_json.to_json()

data_csv = pd.read_csv (r'../DataFromClass/data2/vehicle4.csv')
data_csv.to_json (r'../DataFromClass/data2/vehicle4.json')
data_json = pd.read_json(r'../DataFromClass/data2/vehicle4.json')
data_json = data_json[['vehicle_CO2','vehicle_speed']]
data_json_vehicle4 = data_json.to_json()


#print("json",data_json)

#Path to your certificates, modify this
certificate_formatter = "../../AWS_IoT_Keys/8c59becc2f59513d75e3e5fcffacc10c91fcb2f283ac442728b48a7cfd0b592c-certificate.pem.crt"
#certificate_formatter = "./certificates/device_{}/device_{}.certificate.pem"
key_formatter = "../../AWS_IoT_Keys/8c59becc2f59513d75e3e5fcffacc10c91fcb2f283ac442728b48a7cfd0b592c-private.pem.key"
#key_formatter = "./certificates/device_{}/device_{}.private.pem"


class MQTTClient:
	def __init__(self, device_id, cert, key):
		# For certificate based connection
		self.device_id = str(device_id)
		self.state = 0
		self.client = AWSIoTMQTTClient(self.device_id)
		#TODO 2: modify your broker address
		self.client.configureEndpoint("asz1tjyrc4icy-ats.iot.us-east-1.amazonaws.com", 8883)
		self.client.configureCredentials("../../AWS_IoT_Keys/AmazonRootCA1.cer", key, cert)
		self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
		self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
		self.client.configureConnectDisconnectTimeout(10)  # 10 sec
		self.client.configureMQTTOperationTimeout(5)  # 5 sec
		self.client.onMessage = self.customOnMessage
		

	def customOnMessage(self,message):
		#TODO3: fill in the function to show your received message
		print("client {} received -".format(self.device_id), end = " ")
		print(str(message.payload.decode("utf-8")))
		#Don't delete this line
		self.client.disconnectAsync()


	# Suback callback
	def customSubackCallback(self,mid, data):
		#You don't need to write anything here
	    pass


	# Puback callback
	def customPubackCallback(self,mid):
		#You don't need to write anything here
	    pass


	def publish(self):
		#TODO4: fill in this function for your publish
		self.client.connect()
		print("device id",str(self.device_id), self.device_id)
		if int(self.device_id) == 0 :
			self.client.subscribeAsync("Topic11", 1, ackCallback=self.customSubackCallback)
			self.client.publishAsync("VehicleCO2AndSpeed", data_json_vehicle0, 1, ackCallback=self.customPubackCallback)
		elif int(self.device_id) == 1 :
			self.client.subscribeAsync("Topic22", 1, ackCallback=self.customSubackCallback)
			self.client.publishAsync("VehicleCO2AndSpeed", data_json_vehicle1, 1, ackCallback=self.customPubackCallback)
		elif int(self.device_id) == 2 :
			self.client.subscribeAsync("Topic33", 1, ackCallback=self.customSubackCallback)
			self.client.publishAsync("VehicleCO2AndSpeed", data_json_vehicle2, 1, ackCallback=self.customPubackCallback)
		elif int(self.device_id) == 3 :
			self.client.subscribeAsync("Topic44", 1, ackCallback=self.customSubackCallback)
			self.client.publishAsync("VehicleCO2AndSpeed", data_json_vehicle3, 1, ackCallback=self.customPubackCallback)
		elif int(self.device_id) == 4 :
			self.client.subscribeAsync("Topic55", 1, ackCallback=self.customSubackCallback)
			self.client.publishAsync("VehicleCO2AndSpeed", data_json_vehicle4, 1, ackCallback=self.customPubackCallback)
		


# Don't change the code below
print("wait")
lock = Lock()
data = []
for i in range(5):
	a = pd.read_csv(data_path.format(i))
	data.append(a)
clients = []
for device_id in range(device_st, device_end):
	print("device Id",device_id)
	client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
	clients.append(client)



states_for_test = [3, 0, 0, 0, 4, 0, 0, 1, 0, 0, 0, 4, 4, 0, 0, 3, 2, 3, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
 0, 0, 0, 4, 0, 4, 3, 0, 0, 3, 0, 2, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0,\
  2, 4, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 0, 4, 1, 4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0,\
   0, 1, 0, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0,\
    0, 0, 4, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 2, 0, 0, 0, 0, 0, 0, 2, 0, 4, 0, 3, 0,\
     0, 4, 1, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0,\
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0, 2,\
       0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 2, 0, 0, 0, 0,\
        0, 1, 2, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 4, 1, 0, 3, 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,\
         0, 0, 4, 4, 0, 0, 0, 4, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 1, 2, 0, 0,\
          0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 3, 0, 0, 4, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 4, 0, 0,\
           0, 4, 1, 1, 0, 0, 0, 1, 3, 2, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,\
            2, 0, 2, 2, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 0, 0, 0, 0, 0, 0, 4]
s1,s2,s3,s4 = [],[],[],[]
for i in range(device_st,device_end):
	if i < 500:
		clients[i].state = states_for_test[i]
		if states_for_test[i] == 1: s1.append(i)
		elif states_for_test[i] == 2: s2.append(i)
		elif states_for_test[i] == 3: s3.append(i)
		elif states_for_test[i] == 4: s4.append(i)


print("Users at state 1: ", s1)
print("Users at state 2: ", s2)
print("Users at state 3: ", s3)
print("Users at state 4: ", s4)
 


print("send now?")
x = input()
if x == "s":
	for i,c in enumerate(clients):
		c.publish()
	# print("done")
elif x == "d":
	for c in clients:
		c.disconnect()
		print("All devices disconnected")
else:
	print("wrong key pressed")

time.sleep(10)





