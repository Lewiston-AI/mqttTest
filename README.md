# mqttTest
A simple mqtt broker publisher and subscriber. It publishes topics to an existing broker and subscribes to those same topics. 

This application requires you have access to a running MQTT Broker. This has been tested against a HiveMQ broker. The application can run as a publisher and subscriber. You can also run as separate publisher and subscriber; two processes and marginally more interesting.

Configuration is loaded via a json file. The json format is output in the help (-h). 

This project was built from the excellent MQTT documentation and repo here: https://github.com/eclipse/paho.mqtt.python I am sure the examples in that repo are very good and cover many more aspects.
