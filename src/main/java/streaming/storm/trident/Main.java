package streaming.storm.trident;

import storm.trident.TridentTopology;
import streaming.storm.FileMetadata;
import streaming.storm.FileSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


public class Main {

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		topology.newStream("streaming", new FileSpout("C:/temp")).each(new Fields("swift"), new FileMetadataFunction(), new Fields("metadata", "content"))
		.partitionPersist(new MongoDBState.Factory(), new Fields("metadata", "content"), new MongoDBStateUpdater()).parallelismHint(2);
		
		Config conf = new Config(); 
		conf.setDebug(true); 
		conf.setNumWorkers(2);
		conf.registerSerialization(FileMetadata.class);

		LocalCluster cluster = new LocalCluster(); 
		cluster.submitTopology("test", conf, topology.build()); 
		Utils.sleep(10000); 
		cluster.killTopology("test"); 
		cluster.shutdown();

	}

}
