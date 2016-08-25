package com.github.rtempleton.cdr_storm.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class DimLookupBolt implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DimLookupBolt.class);
	private final List<String> inputFields;
	private OutputCollector collector;
	
	private final Map<Integer, Integer> custCache = new HashMap<Integer, Integer>();
	private final Map<Integer, Integer> vendCache = new HashMap<Integer, Integer>();
	private final Map<Integer, Integer> releaseCache = new HashMap<Integer, Integer>();
	
	public DimLookupBolt (Properties props, List<String> inputFields){
		this.inputFields = inputFields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		try{
			Connection con = DriverManager.getConnection("jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure");
			
			String query = "select CUSTOMER_TRUNK_ID, CUST_ID from CDRDWH.CUSTOMER_DIM";
			PreparedStatement stmt = con.prepareStatement(query);
			ResultSet rset = stmt.executeQuery();
			while (rset.next()){
				this.custCache.put(rset.getInt(1), rset.getInt(2));
			}
			stmt.close();
			
			query = "select VENDOR_TRUNK_ID, VEND_ID from CDRDWH.VENDOR_DIM";
			stmt = con.prepareStatement(query);
			rset = stmt.executeQuery();
			while (rset.next()){
				this.vendCache.put(rset.getInt(1), rset.getInt(2));
			}
			stmt.close();
			
			query = "select RELEASE_CODE_NUMBER, REL_CODE_ID from CDRDWH.RELEASE_CODE_DIM";
			stmt = con.prepareStatement(query);
			rset = stmt.executeQuery();
			while (rset.next()){
				this.releaseCache.put(rset.getInt(1), rset.getInt(2));
			}
			stmt.close();
			
			con.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}

	}

	@Override
	public void execute(Tuple input) {
		 
		
		Integer custId = custCache.get(input.getIntegerByField("I_tg_id"));
		if (custId == null)
			custId = 0;
		
		Integer vendId = vendCache.get(input.getIntegerByField("E_tg_id"));
		if (vendId == null)
			vendId = 0;
		
		Integer custRelId = releaseCache.get(input.getIntegerByField("I_rel_cause"));
		if (custRelId == null)
			custRelId = 0;
		
		Integer vendRelId = releaseCache.get(input.getIntegerByField("E_rel_cause"));
		if (vendRelId == null)
			vendRelId = 0;
		
		

		
		List<Object> vals = input.getValues();
		vals.add(custId);
		vals.add(vendId);
		vals.add(custRelId);
		vals.add(vendRelId);
		
		collector.emit(input, vals);
		collector.ack(input);

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//append the field names that will be added by this bolt
		final ArrayList<String> outputFields = new ArrayList<String>(this.inputFields);
		outputFields.add("cust_id");
		outputFields.add("vend_id");
		outputFields.add("cust_rel_id");
		outputFields.add("vend_rel_id");
		declarer.declare(new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
