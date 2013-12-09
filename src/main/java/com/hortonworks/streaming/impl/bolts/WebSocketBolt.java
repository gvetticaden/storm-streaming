package com.hortonworks.streaming.impl.bolts;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import poc.hortonworks.domain.transport.TruckDriverViolationEvent;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WebSocketBolt implements IRichBolt {


	private static final long serialVersionUID = -5319490672681173657L;
	private static final Logger LOG = Logger.getLogger(WebSocketBolt.class);
	
	private OutputCollector collector;
	private Properties config;
	private String user;
	private String password;
	private String activeMQConnectionString;
	private String topicName;

	
	
	public WebSocketBolt(Properties config) {
		this.config = config;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.user = config.getProperty("notification.topic.user");
		this.password = config.getProperty("notification.topic.password");
		this.activeMQConnectionString = config.getProperty("notification.topic.connection.url");
		this.topicName = config.getProperty("notification.topic.events.name");		
	}

	@Override
	public void execute(Tuple input) {
		LOG.info("About to process tuple["+input+"]");

		
		int driverId = input.getIntegerByField("driverId");
		int truckId = input.getIntegerByField("truckId");
		Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
		long eventTimeLong = eventTime.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat();
		String timeStampString = sdf.format(eventTimeLong);		
		String eventType = input.getStringByField("eventType");
		double longitude = input.getDoubleByField("longitude");
		double latitude = input.getDoubleByField("latitude");		
		long numberOfInfractions = input.getLongByField("incidentTotalCount");
		
		String truckDriverEventKey = driverId + "|" + truckId;
		TruckDriverViolationEvent driverInfraction = new TruckDriverViolationEvent(truckDriverEventKey, driverId, truckId, eventTimeLong, timeStampString, longitude, latitude, eventType, numberOfInfractions);
		ObjectMapper mapper = new ObjectMapper();
		String event;
		try {
			event = mapper.writeValueAsString(driverInfraction);
		} catch (Exception e) {
			LOG.error("Error converting TruckDriverViolationEvent to JSON" );
			return;
		}
		
		sendDriverInfractionEventToTopic(event);
		collector.ack(input);
				

	}

	private void sendDriverInfractionEventToTopic(String event) {
		Session session = null;
		try {
			session = createSession();
			TextMessage message = session.createTextMessage(event);
			getTopicProducer(session).send(message);
		} catch (JMSException e) {
			LOG.error("Error sending TruckDriverViolationEvent to topic", e);
			return;
		}finally{
			if(session != null) {
				try {
					session.close();
				} catch (JMSException e) {
					LOG.error("Error cleaning up ActiveMQ resources", e);
				}				
			}

		}
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private MessageProducer getTopicProducer(Session session) {
		try {
			Topic topicDestination = session.createTopic(topicName);
			MessageProducer topicProducer = session.createProducer(topicDestination);
			topicProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			return topicProducer;
		} catch (JMSException e) {
			LOG.error("Error creating producer for topic", e);
			throw new RuntimeException("Error creating producer for topic");
		}
	}	
	
	private Session createSession() {
		
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password,activeMQConnectionString);
			Connection connection = connectionFactory.createConnection();
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			return session;
		} catch (JMSException e) {
			LOG.error("Error configuring ActiveMQConnection and getting session", e);
			throw new RuntimeException("Error configuring ActiveMQConnection");
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}		

}
