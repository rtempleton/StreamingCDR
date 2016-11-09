package com.github.rtempleton.cdr_storm.bolts;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.github.rtempleton.poncho.StormUtils;

public class PutHBaseFact3 implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(PutHBaseFact3.class);
	private OutputCollector collector;
	
	private long cntr = Long.MAX_VALUE;
	
	private Connection con;
	private PreparedStatement stmt;
	private final String JDBCConString;
	private final boolean USE_BATCH;
	private static final int BATCH_SIZE = 100;
	private long recordsBatched = 0;
	private final Calendar cal = Calendar.getInstance(TimeZone.getDefault());
	
	public PutHBaseFact3(Properties props, List<String> inputFields){
		JDBCConString = StormUtils.getRequiredProperty(props, "JDBCConString");
		USE_BATCH = true;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		try{
			con = DriverManager.getConnection(JDBCConString);
			con.setAutoCommit(false);
			stmt = con.prepareStatement("select min(id) from cdrdwh.cdr_fact2");
			ResultSet rset = stmt.executeQuery();
			rset.next();
			if(rset.getLong(1)!=0 & rset.getLong(1)<cntr)
				cntr=rset.getLong(1)-1;
			stmt.clearParameters();
			
			final String query = "upsert into CDRDWH.CDR_FACT2 values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
			stmt = con.prepareStatement(query);
		}catch(Exception e){
			System.out.println("Prepare - " + e.getMessage());
			System.exit(-1);
		}
			

	}

	Integer x, y;
	@Override
	public void execute(Tuple input) {
		
//		call_ts, geo_id, cust_id, vend_id, cust_rel_id, vend_rel_id, route, connect, early_event, call_duration_cust, i_pdd, e_pdd, orig_number, term_number
		
		try {
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
			
			if (USE_BATCH) {
				batchUpdate(stmt);
			} else {
				normalUpdate(stmt);
			}
		} catch (Exception e) {
			logger.error("Execute - " + e.getMessage());
		}
		collector.ack(input);

	}

	void normalUpdate(PreparedStatement stmt) throws Exception {
		stmt.executeUpdate();
		con.commit();
	}

	void batchUpdate(PreparedStatement stmt) throws Exception {
		stmt.addBatch();
		recordsBatched++;
		if (recordsBatched % BATCH_SIZE == 0) {
			executeBatchAndWarn(stmt);
			recordsBatched = 0;
		}
	}

	void executeBatchAndWarn(PreparedStatement stmt) throws Exception {
		int[] updates = stmt.executeBatch();
		long updatesInBatch = 0l;
		for (int update : updates) {
			if (update > 0) {
				updatesInBatch += update;
			} else {
				logger.warn("Saw bad update in batch with return value: " + update);
			}
		}
		con.commit();
		logger.info("Updates added in one batch - " + updatesInBatch);
	}

	@Override
	public void cleanup() {
		try {
			if (recordsBatched > 0) {
				executeBatchAndWarn(stmt);
			}
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
