package com.hortonworks.streaming.impl.topologies;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.hortonworks.streaming.impl.bolts.SimpleHBaseBoltV2;
import com.hortonworks.streaming.impl.bolts.TruckEventRuleBolt;
import com.hortonworks.streaming.impl.kafka.TruckScheme2;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.hbase.bolts.HBaseBolt;
import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TruckEventProcessorKafkaTopology extends BaseTruckEventTopology {
	
	private static final Logger LOG = Logger.getLogger(TruckEventProcessorKafkaTopology.class);
	private static final  String TABLE_NAME = "truck_driver_danagerous_events";
	private static final String COLUMN_FAMILY_NAME = "events";	
	
	public TruckEventProcessorKafkaTopology(String configFileLocation) throws Exception {
		
		super(configFileLocation);			
	}

	private void buildAndSubmit() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		KafkaSpout kafkaSpout = constructKafkaSpout();
		
		int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
		int boltCount = Integer.valueOf(topologyConfig.getProperty("bolt.thread.count"));
		
		builder.setSpout("kafkaSpout", kafkaSpout, spoutCount);
		
		/*
		 * Send truckEvents from same driver to the same bolt instances to maintain accuracy of eventCount per truck/driver
		 */
		builder.setBolt("truck_event_rule_bolt", 
						new TruckEventRuleBolt(topologyConfig), boltCount)
						.fieldsGrouping("kafkaSpout", new Fields("driverId"));
		
		SimpleHBaseBoltV2 hbaseBolt = new SimpleHBaseBoltV2(topologyConfig);
		//HBaseBolt hbaseBolt = new HBaseBolt(constructHBaseTupleConfig());
		
		builder.setBolt("hbaseBolt", hbaseBolt, 2 ).shuffleGrouping("kafkaSpout");
		
		
		/** This conf is for Storm and it needs be configured with things like the following:
		 * 	Zookeeper server, nimbus server, ports, etc... All of this configuration will be picked up
		 * in the ~/.storm/storm.yaml file that will be located on each storm node.
		 */
		Config conf = new Config();
		conf.setDebug(true);	
		/* Set the number of workers that will be spun up for this topology. 
		 * Each worker represents a JVM where executor thread will be spawned from */
		Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("storm.trucker.topology.workers"));
		conf.put(Config.TOPOLOGY_WORKERS, topologyWorkers);
		
		try {
			StormSubmitter.submitTopology("truck-event-processor", conf, builder.createTopology());	
		} catch (Exception e) {
			LOG.error("Error submiting Topology", e);
		}
			
	}

	private TupleTableConfig constructHBaseTupleConfig() {
		TupleTableConfig config = new TupleTableConfig(TABLE_NAME, "eventKey");
		config.addColumn(COLUMN_FAMILY_NAME, "driverId");
		config.addColumn(COLUMN_FAMILY_NAME, "truckId");
		config.addColumn(COLUMN_FAMILY_NAME, "eventTime");
		config.addColumn(COLUMN_FAMILY_NAME, "eventType");
		config.addColumn(COLUMN_FAMILY_NAME, "longitude");
		config.addColumn(COLUMN_FAMILY_NAME, "latitude");
		return config;
	}

	/**
	 * Construct the KafkaSpout which comes from the jar storm-kafka-0.8-plus
	 * @return
	 */
	private KafkaSpout constructKafkaSpout() {
		KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
		return kafkaSpout;
	}

	/**
	 * Construct 
	 * @return
	 */
	private SpoutConfig constructKafkaSpoutConf() {
		BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
		String topic = topologyConfig.getProperty("kafka.topic");
		String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
		String consumerGroupId = topologyConfig.getProperty("kafka.consumer.group.id");
		
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
		
		/* Custom TruckScheme that will take Kafka message of single truckEvent 
		 * and emit a 2-tuple consisting of truckId and truckEvent. This driverId
		 * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
		spoutConfig.scheme = new SchemeAsMultiScheme(new TruckScheme2());
		
		return spoutConfig;
	}
	
	public static void main(String[] args) throws Exception {
		String configFileLocation = args[0];
		TruckEventProcessorKafkaTopology truckTopology = new TruckEventProcessorKafkaTopology(configFileLocation);
		truckTopology.buildAndSubmit();
		
	}	

}



