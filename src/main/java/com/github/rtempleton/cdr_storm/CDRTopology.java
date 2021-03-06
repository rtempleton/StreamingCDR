package com.github.rtempleton.cdr_storm;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.spout.RawScheme;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;

import com.github.rtempleton.cdr_storm.bolts.CallStatOrigTermBolt;
import com.github.rtempleton.cdr_storm.bolts.DimLookupBolt;
import com.github.rtempleton.cdr_storm.bolts.EnrichDomesticCallsBolt;
import com.github.rtempleton.cdr_storm.bolts.EnrichInternationalCallsBolt;
import com.github.rtempleton.cdr_storm.bolts.PDDandEarlyEvents;
import com.github.rtempleton.cdr_storm.bolts.PhoenixFactWriter;
import com.github.rtempleton.cdr_storm.bolts.TimeBucketBolt;
import com.github.rtempleton.poncho.SelectFieldsBolt;
import com.github.rtempleton.poncho.SelectFieldsBolt.SelectType;
import com.github.rtempleton.poncho.StormUtils;
import com.github.rtempleton.poncho.io.ParseDelimitedTextBolt;





public class CDRTopology{
	
	private static final Logger Log = Logger.getLogger(CDRTopology.class);
	private static final String thisName = CDRTopology.class.getSimpleName();
	private final Properties topologyConfig;

	
	public CDRTopology(String properties) throws Exception{
		topologyConfig = StormUtils.readProperties(properties);
	}

	
	
	public static void main(String[] args) throws Exception{
		if (args.length<1){
			usage();
		}
		
		CDRTopology topo = new CDRTopology(args[0]);
		topo.submit(topo.compose());
	}
	
	
	
	protected TopologyBuilder compose() throws Exception{
		
		TopologyBuilder builder = new TopologyBuilder();
		
		String spoutParallelism = topologyConfig.getProperty("spout.parallelism", "1");
		int parallelismHint = Integer.parseInt(spoutParallelism);
		
		final String bootStrapServers = StormUtils.getRequiredProperty(topologyConfig, "kafka.bootStrapServers");
		final String topic = StormUtils.getRequiredProperty(topologyConfig, "kafka.topic");
		final String consumerGroupId = StormUtils.getRequiredProperty(topologyConfig, "kafka.consumerGroupId");
		
		
		@SuppressWarnings("rawtypes")
		KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder(bootStrapServers, topic)
		        .setGroupId(consumerGroupId)
		        .setOffsetCommitPeriodMs(10_000)
		        .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
		        .setMaxUncommittedOffsets(1000000)
		        .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
		                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10)))
		        .build();
		
		// 1. spout reads data
		@SuppressWarnings("unchecked")
		KafkaSpout<String, String> spout = new KafkaSpout<String, String>(spoutConf);
		builder.setSpout("spout", spout, parallelismHint);
		
		//2
		ParseDelimitedTextBolt reader = new ParseDelimitedTextBolt(topologyConfig, StormUtils.getOutputfields(spout));
		builder.setBolt("reader", reader, parallelismHint).localOrShuffleGrouping("spout");
		
		//3
		CallStatOrigTermBolt callStat = new CallStatOrigTermBolt(topologyConfig, StormUtils.getOutputfields(reader));
		builder.setBolt("callStat", callStat).localOrShuffleGrouping("reader");
		
		//4
		//The timebucket bolt has two output streams: Domestic Calls and International calls
		TimeBucketBolt timebucket = new TimeBucketBolt(topologyConfig, StormUtils.getOutputfields(callStat));
		builder.setBolt("timebucket", timebucket).localOrShuffleGrouping("callStat");
		
		
//		IRichBolt logger = new ConsoleLoggerBolt(topologyConfig, StormUtils.getOutputfields(timebucket, TimeBucketBolt.DOMESTIC_STREAM));
//		builder.setBolt("consoleLogger", logger).localOrShuffleGrouping("timebucket", TimeBucketBolt.DOMESTIC_STREAM);
		
		//5a
		EnrichInternationalCallsBolt intlBolt = new EnrichInternationalCallsBolt(topologyConfig, StormUtils.getOutputfields(timebucket, TimeBucketBolt.INTERNATIONAL_STREAM));
		builder.setBolt("intlBolt", intlBolt).localOrShuffleGrouping("timebucket", TimeBucketBolt.INTERNATIONAL_STREAM);
		
		//5b
		EnrichDomesticCallsBolt domBolt = new EnrichDomesticCallsBolt(topologyConfig, StormUtils.getOutputfields(timebucket, TimeBucketBolt.DOMESTIC_STREAM));
		builder.setBolt("domBolt", domBolt).localOrShuffleGrouping("timebucket", TimeBucketBolt.DOMESTIC_STREAM);
		
		//6
		PDDandEarlyEvents pdd = new PDDandEarlyEvents(topologyConfig, StormUtils.getOutputfields(intlBolt));
		builder.setBolt("pdd", pdd)
			.localOrShuffleGrouping("intlBolt")
			.localOrShuffleGrouping("domBolt");
		
		//7
		DimLookupBolt dimlookup = new DimLookupBolt(topologyConfig, StormUtils.getOutputfields(pdd));
		builder.setBolt("dimlookup", dimlookup).localOrShuffleGrouping("pdd");
		
		//list the fields we want to keep in the order we want them output.
		List<String> cdr_fact = Arrays.asList(new String[]{"call_ts", "geo_id", "cust_id","vend_id","cust_rel_id","vend_rel_id","route","connect","early_event","call_duration_cust","i_pdd","e_pdd","orig_number","term_number"});
		
		//8
		SelectFieldsBolt select = new SelectFieldsBolt(topologyConfig, StormUtils.getOutputfields(dimlookup), SelectType.RETAIN, cdr_fact);
		builder.setBolt("select", select).localOrShuffleGrouping("dimlookup");
	
		//9
		PhoenixFactWriter writer = new PhoenixFactWriter(topologyConfig, StormUtils.getOutputfields(select));
		builder.setBolt("writer", writer, 1).localOrShuffleGrouping("select");
		
		return builder;
		
	}

	protected void submit(TopologyBuilder builder){

		try {
			StormSubmitter.submitTopology(thisName, topologyConfig, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (AuthorizationException e) {
			e.printStackTrace();
			System.exit(-1);	
		}

	}
	
	
	
	private static void usage(){
		Log.error("You must provide a path to a properties file when running LinearRoadTopology to corrently configure this topology.");
		System.exit(-1);
	}

}
