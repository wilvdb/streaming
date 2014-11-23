package streaming.springxd;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

public class MetadataTransformer {
	
	public FileMetadata transform(File f) {
		FileMetadata metadata = new FileMetadata();

		metadata.setFilename(FilenameUtils.getBaseName(f.getAbsolutePath()));
		metadata.setPath(FilenameUtils.getPath(f.getAbsolutePath()));
		metadata.setProcessingDate(new Date());
		CRC32 crc = new CRC32();
		try {
			metadata.setContent(FileUtils.readFileToString((File) f));
			crc.update(metadata.getContent().getBytes());
			metadata.setChecksum(crc.getValue());
		} catch (IOException e) {
			throw new IllegalStateException("Unable to transform file: " + e.getMessage(), e);
		}
		
		return metadata;
	}

}
