package streaming.storm.trident;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import streaming.storm.FileMetadata;

@SuppressWarnings("rawtypes")
public class MongoDBStateUpdater extends BaseStateUpdater<MongoDBState> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public void updateState(MongoDBState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		System.out.println("toto");
		List<FileMetadata> metadata = new ArrayList<FileMetadata>();
		for (TridentTuple tuple : tuples) {
			Object value = tuple.getValueByField("metadata");
			if(value instanceof FileMetadata) {
				metadata.add((FileMetadata) value);
			}
		}
		state.multiPut(null, metadata);
		
	}
	
	

}
