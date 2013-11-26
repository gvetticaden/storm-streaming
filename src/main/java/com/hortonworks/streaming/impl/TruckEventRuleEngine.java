package com.hortonworks.streaming.impl;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;


import com.hortonworks.streaming.impl.utils.EventMailer;

public class TruckEventRuleEngine implements Serializable {
	
	private static final long serialVersionUID = -5526455911057368428L;
	private static final Logger LOG = Logger.getLogger(TruckEventRuleEngine.class);
	public static final int MAX_UNSAFE_EVENTS = 5;
	
	public Map<Integer, LinkedList<String>> driverEvents = new HashMap<Integer, LinkedList<String>>();
	private String email ;
	private String subject;
	private EventMailer eventMailer;


	public TruckEventRuleEngine(Properties config) {
		
		this.eventMailer = new EventMailer(config);
		
		LOG.info("constructing new TruckEventRuleEngine");
		
		this.email = config.getProperty("notification.email");
		this.subject =  config.getProperty("notification.subject");
		
		LOG.info("Initializing rule engine with email: " + email
				+ " subject: " + subject);		
	}

	public void processEvent(int driverId, int truckId, Timestamp eventTime, String event, double longitude, double latitude) {

		if (!driverEvents.containsKey(driverId))
			driverEvents.put(driverId, new LinkedList<String>());
			
		
		if (!event.equals("Normal")) {
			if (driverEvents.get(driverId).size() < MAX_UNSAFE_EVENTS) {
				driverEvents.get(driverId).push(eventTime + " " + event);
				LOG.info("Driver["+driverId+"] has an unsafe event and now has the following unsfae events " + driverEvents.get(driverId).size());
			}
			else {
				LOG.info("Driver["+driverId+"] has exceed max events...");
				try {
					// In this case they've had more than 5 unsafe events
					LOG.info("UNSAFE DRIVING DETECTED FOR DRIVER ID: "
							+ driverId);
					StringBuffer events = new StringBuffer();
					for (String unsafeEvent : driverEvents.get(driverId)) {
						events.append(unsafeEvent + "\n");
					}
					eventMailer.sendEmail(email, email, subject,
							"We've identified 5 unsafe driving events for driver: "
									+ driverId + "\n\n" + events.toString());
				} catch (Exception e) {
					LOG.error("Error occured while sending notificaiton email: "
							+ e.getMessage());
				} finally {
					driverEvents.get(driverId).clear();
				}
			}
		}
	}
}
