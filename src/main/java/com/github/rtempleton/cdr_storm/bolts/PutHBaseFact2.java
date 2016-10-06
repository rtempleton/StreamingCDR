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
//	private final List<String> inputFields;
	private OutputCollector collector;
	
	private int cntr = Integer.MAX_VALUE;
	
	private Connection con;
	private PreparedStatement stmt;
	private final String JDBCConString;
	private final boolean USE_BATCH;
	private static final int BATCH_SIZE = 100;
	private long recordsBatched = 0;
	
	public PutHBaseFact2(Properties props, List<String> inputFields){
//		this.inputFields = inputFields;
		JDBCConString = StormUtils.getRequiredProperty(props, "JDBCConString");
		USE_BATCH = true;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		try{
			con = DriverManager.getConnection(JDBCConString);
			con.setAutoCommit(false);
			final String query = "upsert into CDRDWH.CDR_FACT values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			stmt = con.prepareStatement(query);
		}catch(Exception e){
			System.out.println("Prepare - " + e.getMessage());
			System.exit(-1);
		}
			

	}

	Integer x, y;
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
			stmt.setShort(6, input.getIntegerByField("vend_id").shortValue());
			stmt.setShort(7, input.getIntegerByField("cust_rel_id").shortValue());
			stmt.setShort(8, input.getIntegerByField("vend_rel_id").shortValue());
			stmt.setByte(9, input.getIntegerByField("route").byteValue());
			x = input.getBooleanByField("connect").booleanValue()?1:0;
			stmt.setByte(10, x.byteValue());
			y = input.getBooleanByField("early_event").booleanValue()?1:0;
			stmt.setByte(11, y.byteValue());
			stmt.setDouble(12, input.getFloatByField("call_duration_cust"));
			stmt.setLong(13, input.getLongByField("i_pdd"));
			stmt.setLong(14, input.getLongByField("e_pdd"));
			
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
