package com.github.rtempleton.cdr_storm.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
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

import com.github.rtempleton.operators.trie.LongestSequenceTrie;
import com.github.rtempleton.operators.trie.Trie;
import com.github.rtempleton.poncho.StormUtils;

public class EnrichDomesticCallsBolt implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(EnrichDomesticCallsBolt.class);
	private final List<String> inputFields;
	private final List<Object> results = new ArrayList<Object>();
	private OutputCollector collector;
	
	private final Trie trie = new LongestSequenceTrie();
	private final String JDBCConString;
	
	public EnrichDomesticCallsBolt(Properties props, List<String> inputFields) {
		this.inputFields = inputFields;
		JDBCConString = StormUtils.getRequiredProperty(props, "JDBCConString");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		//populate the Trie with the geogrpahy info from the dim table
		final String query = "select npanxx, geo_id from cdrdwh.geography_dim where dial_code = '1'";
		try{
			Connection con = DriverManager.getConnection(JDBCConString);
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
		// TODO Auto-generated method stub
		
		String npanxx = "0";
		if(input.getStringByField("term_number").length()==11 && input.getStringByField("term_number").substring(0, 1) == "1")
			npanxx=input.getStringByField("term_number").substring(1, 7);
		else if(input.getStringByField("term_number").length()==10 && input.getStringByField("term_number").substring(0, 1) != "1")
			npanxx=input.getStringByField("term_number").substring(0, 6); 
		this.trie.walkTrie(npanxx, results);
			
		int geo_id = 0;
		this.trie.walkTrie(npanxx, results);
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
