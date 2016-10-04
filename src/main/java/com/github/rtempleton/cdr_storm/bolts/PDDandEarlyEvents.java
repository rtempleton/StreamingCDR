package com.github.rtempleton.cdr_storm.bolts;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.joda.time.DateTime;

public class PDDandEarlyEvents implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(PDDandEarlyEvents.class);
	private final List<String> inputFields;
	private Fields outputFields;
	private OutputCollector collector;
	
	private int e_tg_id_pos, e_rel_cause_pos, i_pdd_pos, e_pdd_pos, connect_pos, ee_pos, rte_pos, call_dur_pos = -1;
	private boolean setup = false;
	
	public PDDandEarlyEvents(Properties props, List<String> inputFields) {
		this.inputFields = inputFields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		
		//find the positions of the output fields we will be populating only once!
		if(!setup){
			i_pdd_pos = outputFields.fieldIndex("i_pdd");
			e_pdd_pos = outputFields.fieldIndex("e_pdd");
			connect_pos = outputFields.fieldIndex("connect");
			ee_pos = outputFields.fieldIndex("early_event");
			rte_pos = outputFields.fieldIndex("route");
			e_tg_id_pos = outputFields.fieldIndex("E_tg_id");
			e_rel_cause_pos = outputFields.fieldIndex("E_rel_cause");
			call_dur_pos = outputFields.fieldIndex("call_duration_cust");
			setup = true;
		}
		
		DateTime ingressEnd = null;
		if (input.getValueByField("I_acm_t")!=null)
			ingressEnd = new DateTime(input.getValueByField("I_acm_t"));
		else if (input.getValueByField("E_setup_t")!=null)
			ingressEnd = new DateTime(input.getValueByField("E_setup_t"));
		else if (input.getValueByField("I_rel_t")!=null)
			ingressEnd = new DateTime(input.getValueByField("I_rel_t"));
		
		long I_PDD = 0l;
		if(ingressEnd!=null && input.getValueByField("I_iam_t")!=null){
			I_PDD = ingressEnd.getMillis() - new DateTime(input.getValueByField("I_iam_t")).getMillis();
		}
		
		DateTime egressEnd = null;
		if (input.getValueByField("E_acm_t")!=null)
			egressEnd = new DateTime(input.getValueByField("E_acm_t"));
		else if (input.getValueByField("E_rel_t")!=null)
			egressEnd = new DateTime(input.getValueByField("E_rel_t"));
		
		long E_PDD = 0l;
		if(egressEnd!=null && input.getValueByField("E_setup_t")!=null){
			E_PDD = egressEnd.getMillis() - new DateTime(input.getValueByField("E_setup_t")).getMillis();
		}
		
		
		
		
		//pivot early events
		int cntr = 0;
		String early_events = input.getStringByField("Early_events");
		
		if(!early_events.isEmpty()){
			
			String[] res = early_events.split("<G N=\"HC\"");
			String trunkGrp,relCause;
			int beginIndex,endIndex;
			
			for(int i=1;i<res.length;i++){
				
				//Set the outputs based on the initial inputs, we will overwrite the individual output field values below
				List<Object> vals = new ArrayList<Object>(input.getValues());
				//add objects to the vals list for the values we are about to set
				for(int j=0;j<outputFields.size()-inputFields.size();j++){
					vals.add(null);
				}
				
				beginIndex = res[i].indexOf("TG=\"");
				if(beginIndex != -1){
					trunkGrp = res[i].substring(beginIndex+4);
					endIndex = trunkGrp.indexOf("\"");
					trunkGrp = (endIndex!=-1) ? trunkGrp.substring(0, endIndex) : "0";
				}else{
					trunkGrp = "0";
				}
					
				beginIndex = res[i].indexOf("Cause=\"");
				if(beginIndex != -1){
					relCause = res[i].substring(beginIndex+7);
					endIndex = relCause.indexOf("\"");
					relCause = (endIndex!=-1) ? relCause.substring(0, endIndex) : "0";
				}else{
					relCause = "0";
				}
				
				//only push records when both fields were identified
				if(trunkGrp == "0" || relCause == "0")
					continue;
				
				//set the earlyEvent data
				
				
				//push the inputs to the outputs - we'll then overwrite the specific fields below				
				try{
					vals.set(e_tg_id_pos, Integer.parseInt(trunkGrp));
				}catch(Exception e){
					continue;
				}
				
				try{
					vals.set(e_rel_cause_pos, Integer.parseInt(relCause));
				}catch(Exception e){
					continue;
				}
				
				//push false connected for each "attempt" and true to indicate early event
				vals.set(connect_pos, Boolean.FALSE);
				vals.set(ee_pos, Boolean.TRUE);
				vals.set(rte_pos, ++cntr);
				//push nulls on the outIppd and outEpdd for the early events since we don't want to duplicate the values
				//which effect downstream aggregations
				vals.set(i_pdd_pos, 0l);
				vals.set(e_pdd_pos, 0l);
				vals.set(call_dur_pos, 0f);
				
				//don't ack here, only on the last one...
				collector.emit(vals);
			}
		}
		
		//Set the outputs based on the initial inputs, we will overwrite the individual output field values below
		List<Object> vals = new ArrayList<Object>(input.getValues());
		//add objects to the vals list for the values we are about to set
		for(int i=0;i<outputFields.size()-inputFields.size();i++){
			vals.add(null);
		}
			
		String t = input.getStringByField("Call_status");
		vals.set(connect_pos, t.equals("S"));
		vals.set(ee_pos, Boolean.FALSE);
		vals.set(rte_pos, ++cntr);
		
		vals.set(i_pdd_pos, I_PDD);
		vals.set(e_pdd_pos, E_PDD);
		vals.set(call_dur_pos, input.getValue(call_dur_pos));
		
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
		outputFields.add("i_pdd");
		outputFields.add("e_pdd");
		outputFields.add("connect");
		outputFields.add("early_event");
		outputFields.add("route");
		this.outputFields = new Fields(outputFields);
		declarer.declare(this.outputFields);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
