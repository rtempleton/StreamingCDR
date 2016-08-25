package com.github.rtempleton.cdr_storm.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * Bolt that performs the following actions
 * 1. Filters out Records where Call_status is not equal to 'S' or 'U'
 * 2. Derives the originating number and appends the output with 'orig_number' field
 * 3. Derives the terminating number and appends the output with 'term_number' fields
 * 
 * @author rtempleton
 *
 */
public class CallStatOrigTermBolt implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private final List<String> inputFields;
	
	private final String callStatField = "Call_status";
	private final String orig_number = "orig_number";
	private final String term_number = "term_number";
	
	public CallStatOrigTermBolt(Properties props, List<String> inputFields){
		this.inputFields = inputFields;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		//filter out non S or U Call_status records
		if(input.getStringByField(callStatField)!= null && input.getStringByField(callStatField).equals("S") || input.getStringByField(callStatField).equals("U")){
			List<Object> vals = input.getValues();
			//add the orig_number value
			String orig = (input.getStringByField("CN") != null && !input.getStringByField("CN").isEmpty()) ? input.getStringByField("CN") : input.getStringByField("CPN");
			vals.add(orig);
			//add the term_number value
			String term = (input.getStringByField("R_lrn") != null && !input.getStringByField("R_lrn").isEmpty()) ? input.getStringByField("R_lrn") : input.getStringByField("DN");
			vals.add(term);
			
			collector.emit(input, vals);
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//append the field names that will be added by this bolt
		final ArrayList<String> outputFields = new ArrayList<String>(this.inputFields);
		outputFields.add(orig_number);
		outputFields.add(term_number);
		declarer.declare(new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
