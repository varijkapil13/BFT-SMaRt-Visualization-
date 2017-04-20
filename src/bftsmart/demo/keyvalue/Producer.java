package bftsmart.demo.keyvalue;

import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.json.*;


public class Producer {
	public static Properties props = new Properties();
	
	public Producer() {
		
		   props.put("bootstrap.servers", "192.168.12.57:9092"); 		//Assign kafka server IP address
		   props.put("acks", "all");                                    //If the request fails, the producer can automatically retry,
		   props.put("retries", 0);                                     //Specify buffer size in config
		   props.put("batch.size", 16384);                              //Reduce the no of requests less than 0    
		   props.put("linger.ms", 1);                     
		   props.put("buffer.memory", 33554432);                        //Total amount of memory available to the producer for buffering. 
		   props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		   props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		   
	}
	
	
	public void sendMessage(int[] targets, int sender,Boolean toClient, String typeOfMessage, int epoch) {
		//System.out.println("Normal");
		//System.out.println("Targets:" + targets+ " Sender:" + sender + " toClient:" + toClient + " id:" + epoch);
		
		long time = System.currentTimeMillis(); //(int)(cal.getTimeInMillis()/1000);
		 //System.out.println("Time from the server" + time);
		for (int i = 0; i<targets.length; i++) {

			JSONObject data = new JSONObject();
			//JSONArray targetsArray = new JSONArray(targets);
			data.put("sender", sender);
			data.put("toClient", toClient);
			data.put("messageId", epoch);
			data.put("receiver", targets[i]);
			data.put("time-sent", time);
			data.put("messagetype", typeOfMessage);
			data.put("data", "sendmessage");
			this.sendToKafka("sendMessage", data.toString());
		}
		
	}


	public void orderedRequestFromClientRecieved(int senderId, int messageId) {
		//System.out.println("Ordered Request Recieved at Client Manager");
		
		long time = System.currentTimeMillis(); //(int)(cal.getTimeInMillis()/1000);
		JSONObject data = new JSONObject();
		data.put("sender", senderId);
		data.put("messageId", messageId);
		data.put("time-recieved", time);
		data.put("type", "general");
		data.put("data", "orderedrequestatclientmanager");
		this.sendToKafka("orderedRequestFromClientAtClientManager", data.toString());
	}
////// Used for showing messages on the graph
	public void consensusMessageReceived(String type, int senderId, int receiverId, int messageId, long sentTime) {
		//System.out.println("Consensus messages");
		//System.out.println("type:" + type+ " Sender:" + senderId + " receiverId:" + receiverId + " id:" + messageId);
		
		long time = System.currentTimeMillis(); //(int)(cal.getTimeInMillis()/1000);
		JSONObject data = new JSONObject();
		data.put("sender", senderId);
		data.put("type", type);
		data.put("messageId", messageId);
		data.put("receiver", receiverId);
		data.put("time-sent", sentTime);
		data.put("time-recieved", time);
		data.put("data", "consensus");
		System.out.println("Consenssus message    type:" + type+ " Sender:" + senderId + " receiverId:" + receiverId + " time-sent: " + sentTime + " time-received " + time);
		this.sendToKafka("consensusMessage", data.toString());
	}
	public void unorderedRequestFromClientRecieved(int serverId,int senderId) {
		//System.out.println("Unordered Request Recieved at Server:" + serverId);
		JSONObject data = new JSONObject();
		long time = System.currentTimeMillis();
		data.put("receiver", serverId);
		data.put("sender", 1001);
        data.put("time-recieved", time);
        data.put("type", "Normal");
        data.put("data", "unorderedrquestfromclient");
		this.sendToKafka("unorderedRequestFromClientAtServer", data.toString());
	}

	public void orderedRequestFromClientManagerReceivedAtServer(int serverId, int senderId) {
		//System.out.println("Ordered Request from Client Manager at Server with ID: " + serverId);
		
		long time = System.currentTimeMillis(); //(int)(cal.getTimeInMillis()/1000);
		JSONObject data = new JSONObject();
		data.put("sender", 1001);
		data.put("time-recieved", time);
		data.put("type", "Normal");
		data.put("receiver", serverId);
		data.put("data", "orderedrequestatserver");
		//data.put("messageId", messageId);
		this.sendToKafka("orderedRequestFromClientManagerAtServer", data.toString());
	}

	public void replyAtClientReceived(int sender, int messageId) {
		//System.out.println("Received reply at client from the server- Sender:" + sender + " messageId:" + messageId);
		
		long time = System.currentTimeMillis(); 
		JSONObject data = new JSONObject();
		data.put("sender", sender);
		data.put("receiver", 1001);
		data.put("messageId", messageId);
		data.put("time-recieved", time);
		data.put("type", "Normal");
		data.put("data", "replyatclient");
		this.sendToKafka("replyAtClientReceived", data.toString());
	}

	public void sendToKafka(String topicName, String value) {
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName, value);
		
		producer.send(data);
		producer.close();

	}
public static void main(String[] args) throws Exception{
   
   
	

       /* producer.send(new ProducerRecord<String, String>(
                "test",
                String.format("Random message ")));

                System.out.println("Sent random msg number ");
              
                producer.close();*/

   }
}

