package com.hortonworks.streaming.impl.kafka;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TruckScheme2 implements Scheme{

	private static final long serialVersionUID = -2990121166902741545L;

	private static final Logger LOG = Logger.getLogger(TruckScheme2.class);
	
	@Override
	public List<Object> deserialize(byte[] bytes) {
		try {
			String truckEvent = new String(bytes, "UTF-8");
			String[] pieces = truckEvent.split("\\|");
			
			Timestamp eventTime = Timestamp.valueOf(pieces[0]);
			int truckId = Integer.valueOf(pieces[1]);
			int driverId = Integer.valueOf(pieces[2]);
			String eventType = pieces[3];
			double longitude = Double.valueOf(pieces[4]);
			double latitude = Double.valueOf(pieces[5]);			
			String eventKey = consructKey(driverId, truckId, eventTime);
			
			LOG.info("Creating a Truck Scheme with driverId["+driverId + "] and truckEvent["+truckEvent + "]");
			return new Values(driverId, truckId, eventTime, truckEvent, longitude, latitude, eventKey);
			
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("driverId", "truckId", "eventTime", "eventType", "longitude", "latitude", "eventKey");
		
	}
	
	private String consructKey(int driverId, int truckId, Timestamp ts2) {
		long reverseTime = Long.MAX_VALUE - ts2.getTime();
		String rowKey = driverId+"|"+truckId+"|"+reverseTime;
		return rowKey;
	}	

}