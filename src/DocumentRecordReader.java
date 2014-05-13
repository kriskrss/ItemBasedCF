import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 
 * @author sai Record reader that supports DocumentInputFormat
 */
public class DocumentRecordReader implements RecordReader<Text, Text> {
	private LineRecordReader lineReader;
	private Text lineKey;
	private Text lineValue;
	private LongWritable lineReaderKey;

	public DocumentRecordReader(JobConf conf, FileSplit split)
			throws IOException {
		lineReader = new LineRecordReader(conf, split);
		lineKey = new Text(split.getPath().getName());
		lineValue = lineReader.createValue();
		lineReaderKey = lineReader.createKey();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		if (!lineReader.next(lineReaderKey, lineValue)) {
			return false;
		}
		key.set(lineKey);
		value.set(lineValue);
		return true;
	}

}
