package com.github.rtempleton.cdr_storm.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.github.rtempleton.poncho.StormUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class TimeBucketBolt implements IRichBolt {
	
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(TimeBucketBolt.class);
	private final List<String> inputFields;
	
	public static final String DOMESTIC_STREAM = "domestic";
	public static final String INTERNATIONAL_STREAM = "international";
	
	private final String call_ts = "call_ts";
//	private final String time_id = "time_id";
//	private final String date_id = "date_id";
	private OutputCollector collector;
	
//	private final String JDBCConString;
//	private LoadingCache<String, Integer> cache;
	
	public TimeBucketBolt(Properties props, List<String> inputFields) {
		this.inputFields = inputFields;
//		JDBCConString = StormUtils.getRequiredProperty(props, "JDBCConString");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
//		this.cache = CacheBuilder.newBuilder()
//				.maximumSize(10)
//				.expireAfterAccess(24, TimeUnit.HOURS)
//				.build(new CacheLoader<String, Integer>(){
//					@Override
//					public Integer load(String s){
//						return execute(s);
//					}
//				});
	}

	@Override
	public void execute(Tuple input) {
//		DateTime ts = new DateTime(input.getValueByField("I_iam_t"));
//		float dur = input.getFloatByField("call_duration_cust");
		
		//Inject the current date into the flow rather than using the date from the CDR
		
//		DateTime added = ts.plusSeconds((int)dur);
		DateTime added = DateTime.now(DateTimeZone.forID("America/Chicago"));
//		int time_id = (added.getHourOfDay()*3600 + added.getMinuteOfHour()*60 + added.getSecondOfMinute())/900;
//		String key = added.getYear() + "-" + added.getDayOfYear();
//		int date_id = 0;
//		try {
//			date_id = cache.get(key);
//		} catch (ExecutionException e) {
//			logger.warn(String.format("Exectuion Exception caught for date_id key value of %s", key));
//			date_id = 0;
//		}
		List<Object> vals = input.getValues();
		vals.add(added.getMillis());
//		vals.add(time_id);
//		vals.add(date_id);
		//direct Addr_Nature == 3 calls to domestic output stream, otherwise send them out on the international output stream
		if(input.getStringByField("Addr_Nature").equals("3"))
			collector.emit(DOMESTIC_STREAM, input, vals);
		else
			collector.emit(INTERNATIONAL_STREAM, input, vals);
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
		outputFields.add(call_ts);
//		outputFields.add(time_id);
//		outputFields.add(date_id);
		declarer.declareStream("domestic", new Fields(outputFields));
		declarer.declareStream("international", new Fields(outputFields));
		//declarer.declare(new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
//	private int execute(String key){
//		String[] keys = key.split("-");
//		String query = "select DATE_ID from cdrdwh.date_dim where YEAR_VAL = " + keys[0] + " and DAY_OF_YEAR = " + keys[1];
//		int i =0;
//		try{
//			Connection con = DriverManager.getConnection(JDBCConString);
//			PreparedStatement stmt = con.prepareStatement(query);
//			ResultSet rset = stmt.executeQuery();
//			while (rset.next()){
//				i=rset.getInt(1);
//			}
//			stmt.close();
//			con.close();
//		}catch(Exception e){
//			System.out.println(e.getMessage());
//		}
//		return i;
//	}

}
