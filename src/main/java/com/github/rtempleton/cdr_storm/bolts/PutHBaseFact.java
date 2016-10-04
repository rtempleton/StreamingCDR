package com.github.rtempleton.cdr_storm.bolts;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataTypeFactory;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class PutHBaseFact implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(PutHBaseFact.class);
	private final List<String> inputFields;
	private OutputCollector collector;
	
	private int cntr = Integer.MAX_VALUE;
	private final byte[] COLUMN_FAMILY = Bytes.toBytes("0");
	
	private Connection conn;
	private Table t;
	
	public PutHBaseFact(Properties props, List<String> inputFields){
		this.inputFields = inputFields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "localhost");
		
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			t = conn.getTable(TableName.valueOf("CDRDWH.CDR_FACT"));
		} catch (IOException e) {
			e.printStackTrace();
		}
			

	}

	@Override
	public void execute(Tuple input) {
		
//		geo_id, time_id, cust_id, vend_id, cust_rel_id, vend_rel_id, route, connect, early_event, call_duration_cust, i_pdd, e_pdd, orig_number, term_number, date_id
		
		Put p = new Put(PUnsignedInt.INSTANCE.toBytes(cntr--));
		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("DATE_ID"), PUnsignedSmallint.INSTANCE.toBytes(input.getIntegerByField("date_id")));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("TIME_ID"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("CUST_ID"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("VEND_ID"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("GEO_ID"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("CUSTOMER_RELEASE_CODE_ID"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("VENDOR_RELEASE_CODE_ID"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("CALL_TYPE_ID"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("RTE"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("CNCT"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("EARLY_EVENT"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("DURATION"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("I_PDD"), PLong.INSTANCE.toBytes(new Integer(107)));
//		p.addColumn(COLUMN_FAMILY, Bytes.toBytes("E_PDD"), PLong.INSTANCE.toBytes(new Integer(107)));
		
		try {
			t.put(p);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void cleanup() {
		try {
			t.close();
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
