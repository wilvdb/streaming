package streaming.spark;

import java.io.File;
import java.util.Date;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.function.Function;

public class FilemetadataMapper implements Function<File, FileMetadata> {

	private static final long serialVersionUID = 1L;

	public FileMetadata call(File f) throws Exception {
				FileMetadata metadata = new FileMetadata();
				metadata.setFilename(FilenameUtils.getBaseName(f.getAbsolutePath()));
				metadata.setPath(FilenameUtils.getPath(f.getAbsolutePath()));
				metadata.setProcessingDate(new Date());
//				CRC32 crc = new CRC32();
//				//byte[] content = FileUtils.readFileToByteArray(f);
//				//byte[] content = t.getBytes();
//				crc.update(content);
//				metadata.setChecksum(crc.getValue());
				return metadata;
	}

}
