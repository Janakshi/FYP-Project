import paho.mqtt.client as mqtt


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    client.subscribe("RBL/BPGP/Glaze booth 1 level :")
    print("Connected")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    level = str(msg.payload)
    print("Glaze booth 1 level = " + level[2:-1] + " mm")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("test.mosquitto.org", 1883, 60)

client.loop_forever()
