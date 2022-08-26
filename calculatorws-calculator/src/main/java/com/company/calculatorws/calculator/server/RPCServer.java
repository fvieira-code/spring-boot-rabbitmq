package com.company.calculatorws.calculator.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.company.calculatorws.calculator.handler.MessageHandler;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

@Component
public class RPCServer {

	private ConnectionFactory factory = new ConnectionFactory();
	
	private boolean stop = false;
	
	@Autowired
	private MessageHandler messageHandler;
	
	private Connection connection = null;

	public void start() {
		connection = null;
		Channel channel = null;
		try {
			factory.setHost("localhost");

			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
			channel.basicQos(1);

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

			logger.info(">>>>> Waiting for requests <<<<<");

			while (!stop) {
				String response = null;
				
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				BasicProperties props = delivery.getProperties();
				String corrId = props.getCorrelationId();
				BasicProperties replyProps = new BasicProperties.Builder().correlationId(corrId).build();
				
				try {
					String message = new String(delivery.getBody(),"UTF-8");
					logger.info(String.format("<-- Got from %s : %s", corrId, message));
					
					response = messageHandler.process(message);
					
					logger.info(String.format("--> Sending to %s : %s", corrId, response));
				}
				catch (Exception e){
					response = String.format("ERROR: %s", e.getMessage());
					logger.info(String.format("--> Sending to %s : %s", corrId, response));
				}
				finally {  
					channel.basicPublish( "", props.getReplyTo(), replyProps, response.getBytes("UTF-8"));
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				}
			}
		}
		catch  (Exception e) {
			logger.error("An error has occurred!", e);
		}
		finally {
			stop();
		}
    }
	
	public void stop() {
		stop = true;
		
		if (connection != null) {
			try {
				connection.close();
			}
			catch (Exception ignore) {}
		}
	}
	
	private static final String RPC_QUEUE_NAME = "rpc_queue";
	
	private static final Logger logger = LoggerFactory.getLogger(RPCServer.class);
}
