package streaming.spark;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class FileReceiver extends Receiver<File> {

	private static final long serialVersionUID = 1L;
	
	private String root;
	
	public FileReceiver(StorageLevel storageLevel, String root) {
		super(storageLevel);
		this.root = root;
	}

	@Override
	public void onStart() {
		FileAlterationObserver observer = new FileAlterationObserver(root);
		observer.addListener(new FileModifierListener());
		
		observer.checkAndNotify();
		stop("Everything is loaded");
	}

	@Override
	public void onStop() {
		// nothing to do

	}

	class FileModifierListener extends FileAlterationListenerAdaptor {
		
		@Override
		public void onFileCreate(File file) {
			store(file);
		}
	}
}
