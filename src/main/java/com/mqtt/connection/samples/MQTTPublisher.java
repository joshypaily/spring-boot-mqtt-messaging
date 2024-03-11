package com.mqtt.connection.samples;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

class MQTTPublisher {

    private static MqttClient mqttClient = null;

    public static void main(String[] args){
        
        String clientId = "mqtt-client-305";     
        String url ="tcp://localhost:1883";
        String topic = "topic";

        try (MemoryPersistence persistence = new MemoryPersistence()) {

            mqttClient = new MqttClient(url, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            //set username and password here, if required
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true);
            System.out.println("Connecting to url: "+url);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            int i=0;
            while(i <= 100) {
                String message ="Message  "+i;
                sendMessage(message,topic);
                i++;
                Thread.sleep(3000);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    
   }

    public static void sendMessage(String messageString, String topic){

        System.out.println("Publishing message: "+messageString+", to topic: "+topic);
        MqttMessage message = new MqttMessage(messageString.getBytes());
        message.setQos(0);
        try {
            mqttClient.publish(topic, message);
            System.out.println("Message published");
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
