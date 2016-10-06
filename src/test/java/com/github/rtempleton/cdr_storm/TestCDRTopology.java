package com.github.rtempleton.cdr_storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Ignore;
import org.junit.Test;

import com.github.rtempleton.poncho.TextFileProducer;

public class TestCDRTopology extends AbstractTestCase {
	
	private final String CDR_DATA = getResourcePath("CDRSample.txt");
	private final String TOPOLOGY_CONFIG = getResourcePath("config.properties");
	private final String PRODUCER_CONFIG = getResourcePath("producer.properties");
	private final String CDR_SCHEMA = getResourcePath("cdr_schema.json");
	
	
	@Ignore
	public void loadCDRDataTest(){
		TextFileProducer producer = new TextFileProducer(PRODUCER_CONFIG, CDR_DATA);
		producer.run();
	}
	
	@Test
	public void topologyTest() throws Exception{
		CDRTopology topo = new CDRTopology(TOPOLOGY_CONFIG);
		TopologyBuilder builder = topo.compose();
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		
		cluster.submitTopology("CDRTopology", conf, builder.createTopology());
	
		Thread.sleep(4*60*1000);
		
		cluster.shutdown();
		cluster.killTopology("CDRTopology");
		System.out.println("Finsihed!!!");
	}

}
