package streaming.storm;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class FileSpout extends BaseRichSpout implements FileAlterationListener {

	private SpoutOutputCollector collector;

	private List<File> files = new ArrayList<File>();
	private FileAlterationObserver observer;
	
	public FileSpout(String root) {
		File directory = new File(root);

		observer = new FileAlterationObserver(directory);
		observer.addListener(this);
	}

	public void nextTuple() {
		observer.checkAndNotify();

		for (File file : files) {
			collector.emit(Collections.singletonList((Object) file));
		}
		files.clear();
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("swift"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return super.getComponentConfiguration();
	}

	public void onDirectoryChange(File dir) {
		// TODO Auto-generated method stub

	}

	public void onDirectoryCreate(File dir) {
		
	}

	public void onDirectoryDelete(File arg0) {
		// TODO Auto-generated method stub

	}

	public void onFileChange(File file) {
		synchronized (files) {
			if (!files.contains(file)) {
				files.add(file);
			}
		}
	}

	public void onFileCreate(File file) {
		synchronized (files) {
			if (!files.contains(file)) {
				files.add(file);
			}
		}
	}

	public void onFileDelete(File arg0) {
		// TODO Auto-generated method stub

	}

	public void onStart(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub

	}

	public void onStop(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub

	}

}
