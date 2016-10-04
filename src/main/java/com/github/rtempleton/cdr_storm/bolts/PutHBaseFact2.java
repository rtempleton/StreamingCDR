package com.github.rtempleton.cdr_storm.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.github.rtempleton.poncho.StormUtils;

public class PutHBaseFact2 implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(PutHBaseFact2.class);
	private final List<String> inputFields;
	private OutputCollector collector;
	
	private int cntr = Integer.MAX_VALUE;
	
	private Connection con;
	private PreparedStatement stmt;
	private final String JDBCConString;
	
	public PutHBaseFact2(Properties props, List<String> inputFields){
		this.inputFields = inputFields;
		JDBCConString = StormUtils.getRequiredProperty(props, "JDBCConString");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		try{
			Connection con = DriverManager.getConnection(JDBCConString);
			final String query = "upsert into CDRDWH.CDR_FACT values (?,?,?,?,?)";
			stmt = con.prepareStatement(query);
		}catch(Exception e){
			System.out.println("Prepare - " + e.getMessage());
		}
			

	}

	@Override
	public void execute(Tuple input) {
		
//		geo_id, date_id, time_id, cust_id, vend_id, cust_rel_id, vend_rel_id, route, connect, early_event, call_duration_cust, i_pdd, e_pdd, orig_number, term_number
		
		try {
			stmt.clearParameters();
			stmt.setInt(1, cntr--); //ID
			stmt.setInt(2, input.getIntegerByField("geo_id"));
			stmt.setShort(3, input.getIntegerByField("date_id").shortValue());
			stmt.setByte(4, input.getIntegerByField("time_id").byteValue());
			stmt.setShort(5, input.getIntegerByField("cust_id").shortValue());
			
			logger.info(stmt.executeUpdate());
		} catch (Exception e) {
			logger.error("Execute - " + e.getMessage());
		}
		collector.ack(input);

	}

	@Override
	public void cleanup() {
		try {
			stmt.close();
			con.close();
		} catch (Exception e) {
			logger.info(e.getMessage());
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
