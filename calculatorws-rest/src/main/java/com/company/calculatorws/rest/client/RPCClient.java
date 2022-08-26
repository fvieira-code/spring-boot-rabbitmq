package com.company.calculatorws.rest.client;

import java.util.UUID;

import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

@Component
public class RPCClient {

	private static ConnectionFactory factory = new ConnectionFactory();	
	
	private Connection connection;
	private Channel channel;
	private String requestQueueName = "rpc_queue";
	private String replyQueueName;
	private QueueingConsumer consumer;

	public RPCClient() throws Exception {
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare().getQueue();
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(replyQueueName, true, consumer);
	}

	public String call(String message) throws Exception {
		String response = null;
		
		String corrId = (String) MDC.get("UNIQUE_ID");
		if ( corrId == null )
			corrId = UUID.randomUUID().toString();
		
		logger.info(String.format("%s --> Requesting : %s", corrId, message));
		
		BasicProperties props = new BasicProperties
				.Builder().correlationId(corrId).replyTo(replyQueueName).build();

		channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

		while (true) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrId)) {
				response = new String(delivery.getBody(),"UTF-8");
				break;
			}
		}
		
		logger.info(String.format("%s <-- Got : %s", corrId, response));

		return response;
	}

	public void close() throws Exception {
		connection.close();
	}
	
	private static final Logger logger = LoggerFactory.getLogger(RPCClient.class);
}
