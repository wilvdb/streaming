package streaming.spark;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FileToMongoStream {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FileToMongo").setMaster("local");
		final JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(5000));
	
		JavaDStream<File> files = context.receiverStream(new FileReceiver(StorageLevel.MEMORY_AND_DISK_2(), "C:/localapp/d3"));
		JavaDStream<FileMetadata> metadata = files.map(new FilemetadataMapper());
		metadata.foreach(new MongoPersistor());
		
		context.start();
		context.awaitTermination();
	}
	

}
