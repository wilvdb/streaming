package streaming.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class Main {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new FileSpout("C:/temp"), 1);
		builder.setBolt("filemetadata", new FileMetadataBolt()).shuffleGrouping("input");
		builder.setBolt("extract", new ExtractBolt()).fieldsGrouping("filemetadata", new Fields("metadata", "content"));
		
		Config conf = new Config(); 
		conf.setDebug(true); 
		conf.setNumWorkers(2);
		conf.registerSerialization(FileMetadata.class);

		LocalCluster cluster = new LocalCluster(); 
		cluster.submitTopology("test", conf, builder.createTopology()); 
		Utils.sleep(100000); 
		cluster.killTopology("test"); 
		cluster.shutdown();
	}

}
