package com.hortonworks.streaming.impl.bolts;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.hortonworks.streaming.impl.TruckEventRuleEngine;

public class TruckEventRuleBolt implements IRichBolt {
	
	private static final Logger LOG = Logger.getLogger(TruckEventRuleBolt.class);
	
	private OutputCollector collector;
	private TruckEventRuleEngine ruleEngine;

	public TruckEventRuleBolt(Properties kafkaConfig) {
		this.ruleEngine = new TruckEventRuleEngine(kafkaConfig);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		int driverId = input.getIntegerByField("driverId");
		int truckId = input.getIntegerByField("truckId");
		Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
		String eventType = input.getStringByField("eventType");
		double longitude = input.getDoubleByField("longitude");
		double latitude = input.getDoubleByField("latitude");
		
		LOG.info("Processing truck event["+eventType +"]  for driverId["+ driverId +"], truck[" + truckId +"]");
		ruleEngine.processEvent(driverId, truckId, eventTime, eventType, longitude, latitude);
		collector.ack(input);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}

//return new Fields("driverId", "truckId", "eventTime", "eventType", "longitude", "latitude");