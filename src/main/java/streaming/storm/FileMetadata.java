package streaming.storm;

import java.util.Date;


public class FileMetadata {

	private String filename;
	private String path;
	private long checksum;
	private Date processingDate;
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public long getChecksum() {
		return checksum;
	}
	public void setChecksum(long checksum) {
		this.checksum = checksum;
	}
	public Date getProcessingDate() {
		return processingDate;
	}
	public void setProcessingDate(Date processingDate) {
		this.processingDate = processingDate;
	}
	
	
}
