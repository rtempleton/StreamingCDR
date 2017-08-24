package com.github.rtempleton.cdr_storm.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.esotericsoftware.minlog.Log;
import com.github.rtempleton.poncho.StormUtils;
import com.google.common.base.Stopwatch;

public class PhoenixFactWriter implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(PhoenixFactWriter.class);
	private OutputCollector collector;
	
	private final String JDBCConString;
	
	private final long TICK_TUPLE_SECS;
	private final int BATCH_SIZE;
	private final ArrayList<Tuple> cache;
	private static final Calendar cal = Calendar.getInstance(TimeZone.getDefault());
	
	private long cntr = Long.MAX_VALUE;

	public PhoenixFactWriter(Properties props, List<String> inputFields){
		JDBCConString = StormUtils.getRequiredProperty(props, "JDBCConString");
		TICK_TUPLE_SECS = (props.getProperty("writerFlushFreqSecs")!=null)?Long.parseLong(props.getProperty("writerFlushFreqSecs")):60l;
		BATCH_SIZE = (props.getProperty("FlushBatchSize")!=null)?Integer.parseInt(props.getProperty("FlushBatchSize")):300;
		cache = new ArrayList<Tuple>(BATCH_SIZE);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	

	@Override
	public void execute(Tuple input) {
		
		if (isTickTuple(input)){
			if (!cache.isEmpty())
				flushCache();
		}else{
			cache.add(input);
			if(cache.size()==BATCH_SIZE){
				flushCache();
			}
		}
			
		collector.ack(input);
	}
	
	private static boolean isTickTuple(Tuple tuple) {
	    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
	        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
	
	private void flushCache(){
		Stopwatch stopwatch = Stopwatch.createStarted();
		   
		Integer x,y;
		Connection con = null;
		PreparedStatement stmt = null;
		
		try{
		con = DriverManager.getConnection(JDBCConString);
		con.setAutoCommit(false);
		//add a unique (decrementing) long id to each record
		if(cntr==Long.MAX_VALUE) {
			stmt = con.prepareStatement("select min(id) from cdr_fact");
			ResultSet rset = stmt.executeQuery();
			rset.next();
			if(rset.getLong(1)!=0L)
				cntr=rset.getLong(1)-1;
			rset.close();
			stmt.close();
		}
		
		stmt = con.prepareStatement("upsert into CDR_FACT values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
		
//		call_ts, geo_id, cust_id, vend_id, cust_rel_id, vend_rel_id, route, connect, early_event, call_duration_cust, i_pdd, e_pdd, orig_number, term_number		
		for (Tuple input : cache){
			stmt.clearParameters();
			stmt.setLong(1, cntr--); //ID
			stmt.setTimestamp(2, new Timestamp(input.getLongByField("call_ts")), cal);
			stmt.setInt(3, input.getIntegerByField("geo_id"));
			stmt.setShort(4, input.getIntegerByField("cust_id").shortValue());
			stmt.setShort(5, input.getIntegerByField("vend_id").shortValue());
			stmt.setShort(6, input.getIntegerByField("cust_rel_id").shortValue());
			stmt.setShort(7, input.getIntegerByField("vend_rel_id").shortValue());
			stmt.setByte(8, input.getIntegerByField("route").byteValue());
			x = input.getBooleanByField("connect").booleanValue()?1:0;
			stmt.setByte(9, x.byteValue());
			y = input.getBooleanByField("early_event").booleanValue()?1:0;
			stmt.setByte(10, y.byteValue());
			stmt.setDouble(11, input.getFloatByField("call_duration_cust"));
			stmt.setLong(12, input.getLongByField("i_pdd"));
			stmt.setLong(13, input.getLongByField("e_pdd"));
			
			stmt.addBatch();
		}
		
		//execute the batch of inserts and clear the cache
		int[] updates = stmt.executeBatch();
		cache.clear();
		logger.info("Total responses from executeBatch command: " + updates.length);
		con.commit();
		stmt.close();
		con.close();
		}catch (SQLException e){
			logger.error(e.getStackTrace().toString());
		}finally{
			if(cache.size()>0){
				Log.error(cache.size() + " records in the cache failed to insert");
				cache.clear();
			}
			if(stmt!=null)
				try {
					stmt.close();
				} catch (SQLException e) {
				}
			if(con!=null)
				try {
					con.close();
				} catch (SQLException e) {
				}
		}
		
		stopwatch.stop();
		logger.info("...took " + stopwatch.toString());
	}


	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// configure how often a tick tuple will be sent to our bolt
	    Config conf = new Config();
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_TUPLE_SECS);
	    return conf;
	}

}
