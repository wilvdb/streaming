package streaming.storm.trident;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.MongoTemplate;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;
import streaming.storm.FileMetadata;
import backtype.storm.task.IMetricsContext;

import com.mongodb.MongoClient;

public class MongoDBState<T> implements MapState<T> {

	private final MongoTemplate template;
	
	public MongoDBState(MongoTemplate template) {
		this.template = template;
	}
	
	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 1L;

		@SuppressWarnings("rawtypes")
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {

			MongoClient client;
			try {
				client = new MongoClient();
				MongoTemplate template = new MongoTemplate(client, "trident");
				
				return new MongoDBState(template);
			} catch (UnknownHostException e) {
				throw new IllegalStateException(e);
			}
		}
		
	}

	public List<T> multiGet(List<List<Object>> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@SuppressWarnings("rawtypes")
	public List<T> multiUpdate(List<List<Object>> keys,
			List<ValueUpdater> updaters) {
		// TODO Auto-generated method stub
		return null;
	}

	public void multiPut(List<List<Object>> keys, List<T> vals) {
		template.insert(vals, FileMetadata.class);
		
	}

}
