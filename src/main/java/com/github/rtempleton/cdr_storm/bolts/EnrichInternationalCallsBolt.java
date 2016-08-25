package com.github.rtempleton.cdr_storm.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.github.rtempleton.operators.trie.LongestSequenceTrie;
import com.github.rtempleton.operators.trie.Trie;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class EnrichInternationalCallsBolt implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(EnrichInternationalCallsBolt.class);
	private final List<String> inputFields;
	private final List<Object> results = new ArrayList<Object>();
	private OutputCollector collector;
	
	private final Trie trie = new LongestSequenceTrie();
	
	public EnrichInternationalCallsBolt(Properties props, List<String> inputFields) {
		this.inputFields = inputFields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		
		//populate the Trie with the geogrpahy info from the dim table
		String query = "select dial_code, geo_id from cdrdwh.geography_dim where dial_code > '1'";
		try{
			Connection con = DriverManager.getConnection("jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure");
			PreparedStatement stmt = con.prepareStatement(query);
			ResultSet rset = stmt.executeQuery();
			while (rset.next()){
				this.trie.buildTrie(rset.getString(1), rset.getInt(2));
			}
			stmt.close();
			con.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}

	}

	@Override
	public void execute(Tuple input) {
		
		int geo_id = 0;
		
		this.trie.walkTrie(input.getStringByField("term_number"), results);
		if(results.size()>0){
			geo_id=(int)results.get(0);
			results.clear();
		}
		
		List<Object> vals = input.getValues();
		vals.add(geo_id);
		
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
		outputFields.add("geo_id");
		declarer.declare(new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	


}
