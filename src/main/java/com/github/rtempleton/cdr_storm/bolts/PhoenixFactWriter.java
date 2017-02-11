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

public class PhoenixFactWriter implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(PhoenixFactWriter.class);
	private OutputCollector collector;
	
	private final String JDBCConString;
	
	private static final int TICK_TUPLE_SECS = 300;
	private static final int BATCH_SIZE = 300;
	private static final ArrayList<Tuple> cache = new ArrayList<>(BATCH_SIZE);
	private static final Calendar cal = Calendar.getInstance(TimeZone.getDefault());
	private long timeMarker = 0l;
	
	
	
	public PhoenixFactWriter(Properties props, List<String> inputFields){
		JDBCConString = StormUtils.getRequiredProperty(props, "JDBCConString");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	

	@Override
	public void execute(Tuple input) {
		
		
		//if a tick comes through and there's records in the cache and no new records have arrived in the last 30 seconds, flush the cache
		if (isTickTuple(input)){
			if(cache.size()>0){
				if(cal.getTimeInMillis() - timeMarker > 30000){
					logger.info(cache.size() + " items in the cache older than 300 secs. Flushing");
					flushCache();
					timeMarker = cal.getTimeInMillis();
				}
			}
		}else{
			cache.add(input);
			timeMarker = cal.getTimeInMillis();
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
		
		Integer x,y;
		Connection con = null;
		PreparedStatement stmt = null;
		long cntr = Long.MAX_VALUE;
		
		try{
		con = DriverManager.getConnection(JDBCConString);
		con.setAutoCommit(false);
		//add a unique (decrementing) long id to each record
		stmt = con.prepareStatement("select min(id) from cdrdwh.cdr_fact");
		ResultSet rset = stmt.executeQuery();
		rset.next();
		if(rset.getLong(1)!=0)
			cntr=rset.getLong(1)-1;
		rset.close();
		stmt.close();
		
		stmt = con.prepareStatement("upsert into CDRDWH.CDR_FACT values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
		
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
