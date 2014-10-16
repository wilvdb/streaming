package streaming.storm.trident;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import streaming.storm.FileMetadata;

public class FileMetadataFunction extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
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
			
			collector.emit(value);
		}
		
	}


}
