package streaming.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ExtractBolt extends BaseBasicBolt {

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("content"));

	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String swift = input.getStringByField("content");
		FileMetadata metadata = (FileMetadata) input.getValueByField("metadata");
		
	}

}
