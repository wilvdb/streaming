package streaming.storm;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.mongodb.DB;
import com.mongodb.Mongo;

import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;

public class StormIT {
	
	private static MongodForTestsFactory testsFactory;
	 
    @BeforeClass
    public static void setMongoDB() throws IOException {
        testsFactory = MongodForTestsFactory.with(Version.Main.PRODUCTION);
    }
 
    @AfterClass
    public static void tearDownMongoDB() throws Exception {
        testsFactory.shutdown();
    }
 
    private DB db;
 
    @Before
    public void setUpMongoDB() throws Exception {
        final Mongo mongo = testsFactory.newMongo();
        db = testsFactory.newDB(mongo);
    }
 

	@Test
	public void test() {
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
