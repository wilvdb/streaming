package streaming.storm;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class FileMetadataBolt extends BaseRichBolt {
	
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		Object swift = tuple.getValueByField("swift");
		if(swift instanceof File) {
			File swiftFile = (File) swift;
			List<Object> value = new ArrayList<Object>();
			FileMetadata metadata = new FileMetadata();
			value.add(metadata);
			metadata.setFilename(FilenameUtils.getBaseName(swiftFile.getAbsolutePath()));
			metadata.setPath(FilenameUtils.getPath(swiftFile.getAbsolutePath()));
			metadata.setProcessingDate(new Date());
			CRC32 crc = new CRC32();
			try {
				String content = FileUtils.readFileToString((File) swift);
				value.add(content);
				crc.update(content.getBytes());
				metadata.setChecksum(crc.getValue());
			} catch (IOException e) {
				collector.reportError(e);
			}
			
			collector.emit(tuple, value);
		}
		//collector.ack(tuple);
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("metadata", "content"));

	}

}
