package streaming.spark;

import java.net.UnknownHostException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.MongoClient;

public class MongoPersistor implements Function<JavaRDD<FileMetadata>, Void> {

	private static final long serialVersionUID = 1L;

	transient private MongoTemplate template;

	private void init() {
		if (template == null) {
			try {
				MongoClient client = new MongoClient();
				template = new MongoTemplate(client, "spark");
			} catch (UnknownHostException e) {
				throw new IllegalStateException("Cannot connect to MongoDB", e);
			}
		}
	}

	public Void call(JavaRDD<FileMetadata> v1) throws Exception {
		v1.foreach(new VoidFunction<FileMetadata>() {

			private static final long serialVersionUID = 1L;

			public void call(FileMetadata t) throws Exception {
				init();
				template.insert(t);

			}
		});

		return null;
	}
}
