import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author sai Inputformat to read one file at a time
 */
public class DocumentInputFormat extends FileInputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {
		reporter.setStatus(split.toString());
		return new DocumentRecordReader(conf, (FileSplit) split);
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

}
