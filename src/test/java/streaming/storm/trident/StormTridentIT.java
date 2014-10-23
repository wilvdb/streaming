package streaming.storm.trident;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
import storm.trident.TridentTopology;
import streaming.storm.FileMetadata;
import streaming.storm.FileSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class StormTridentIT {

	private static MongodExecutable mongodExe;
	private static MongodProcess mongod;
	 
    @BeforeClass
    public static void setMongoDB() throws IOException {
        MongodStarter runtime = MongodStarter.getDefaultInstance();
        mongodExe = runtime.prepare(new MongodConfig(Version.Main.PRODUCTION));
        
        mongod = mongodExe.start();
    }
 
    @AfterClass
    public static void tearDownMongoDB() throws Exception {
        mongod.stop();
        mongodExe.stop();
    }
    
	@Test
	public void test() throws UnknownHostException, MongoException {
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
