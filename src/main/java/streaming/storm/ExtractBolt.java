package streaming.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ExtractBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("content"));

	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String content = input.getStringByField("content");
		FileMetadata metadata = (FileMetadata) input.getValueByField("metadata");
		
	}

}
