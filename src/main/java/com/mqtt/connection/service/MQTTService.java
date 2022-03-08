package com.mqtt.connection.service;

import javax.annotation.PostConstruct;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class MQTTService implements MqttCallback {

    @Value("${mqtt.topic}")
    private String topic;
    
    @Value("${mqtt.broker.url}")
    private String url;

    private static MqttClient mqttClient = null;

    @PostConstruct
    public void initilize(){
        
        String clientId = "mqtt-client-304";     

        try (MemoryPersistence persistence = new MemoryPersistence()) {

            mqttClient = new MqttClient(url, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            //set username and password here, if required
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true);
            mqttClient.setCallback(this);
            System.out.println("Connecting to url: "+url);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            //subscribe topic
            mqttClient.subscribe(topic);
            System.out.println("topic "+topic+" subscribed");
            
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
    
   }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("connection lost");
        
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("message received : "+message.toString()+", topic:"+topic);
        
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // TODO Auto-generated method stub
        
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
