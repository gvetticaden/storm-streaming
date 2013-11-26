package com.hortonworks.streaming.impl.bolts;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SimpleHBaseBoltV2 implements IRichBolt {


	private static final long serialVersionUID = 2946379346389650318L;
	private static final Logger LOG = Logger.getLogger(SimpleHBaseBoltV2.class);
	
	private static final  String TABLE_NAME = "truck_driver_danagerous_events";
	private static final String COLUMN_FAMILY_NAME = "events";	
	
	private OutputCollector collector;
	private HConnection connection;
	private HTableInterface table;

	public SimpleHBaseBoltV2(Properties kafkaConfig) {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		try {
			this.connection = HConnectionManager.createConnection(constructConfiguration());
			this.table = connection.getTable(TABLE_NAME);
			// Enable client-side write buffer
		    //this.table.setAutoFlush(false, true);			    
		} catch (Exception e) {
			String errMsg = "Error retrievinging connection and access to table";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}		
	}

	@Override
	public void execute(Tuple input) {
		
		LOG.info("About to insert tuple["+input +"] into HBase...");
		
		int driverId = input.getIntegerByField("driverId");
		int truckId = input.getIntegerByField("truckId");
		Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
		String eventType = input.getStringByField("eventType");
		double longitude = input.getDoubleByField("longitude");
		double latitude = input.getDoubleByField("latitude");
		
		try {
			Put put = constructRow(driverId, truckId, eventTime, eventType,
					latitude, longitude);
			this.table.put(put);
			LOG.info("Success inserting tupele into HBase...");
			
		} catch (Exception e) {
			LOG.error("	Error inserting truck event into HBase", e);
		}		
		//acknowledge even if there is an error
		collector.ack(input);
	}
	
	
	/**
	 * We don't need to set any configuration because at deployment time, it should pick up all configuration from hbase-site.xml 
	 * as long as it in classpath. Note that we store hbase-site.xml in src/main/resources so it will be in the topology jar that gets deployed
	 * @return
	 */
	public static  Configuration constructConfiguration() {
		Configuration config = HBaseConfiguration.create();
//		config.set("hbase.zookeeper.quorum",
//				"gvetticaden-central3.secloud.hortonworks.com");
//		config.set("hbase.zookeeper.property.clientPort", "2181");
//		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		return config;
	}	

	
	private Put constructRow(int driverId, int truckId, Timestamp eventTime, String eventType, double latitude, double longitude ) {
		
		String rowKey = consructKey(driverId, truckId, eventTime);
		System.out.println("Record with key["+rowKey + "] going to be inserted...");
		Put put = new Put(Bytes.toBytes(rowKey));
		
		String driverColumn = "driverId";
		put.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(driverColumn), Bytes.toBytes(driverId));
		
		String truckColumn = "truckId";
		put.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(truckColumn), Bytes.toBytes(truckId));
		
		String eventTimeColumn = "eventTimeColumn";
		long reverseTime = Long.MAX_VALUE - eventTime.getTime();
		put.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(eventTimeColumn), Bytes.toBytes(reverseTime));
		
		String eventTypeColumn = "eventType";
		put.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(eventTypeColumn), Bytes.toBytes(eventType));
		
		String latColumn = "latitudeColumn";
		put.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(latColumn), Bytes.toBytes(latitude));
		
		String longColumn = "longitudeColumn";
		put.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(longColumn), Bytes.toBytes(longitude));

		return put;
	}


	private String consructKey(int driverId, int truckId, Timestamp ts2) {
		long reverseTime = Long.MAX_VALUE - ts2.getTime();
		String rowKey = driverId+"|"+truckId+"|"+reverseTime;
		return rowKey;
	}	
	
	
	@Override
	public void cleanup() {
		try {
			table.close();
			connection.close();
		} catch (Exception  e) {
			LOG.error("Error closing table or connection", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
