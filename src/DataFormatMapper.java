import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author sai Mapper to convert data into standard format
 */
public class DataFormatMapper implements Mapper<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void map(Text fileName, Text line,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] parts = line.toString().split(",");
		String fName = fileName.toString();
		fName = fName.substring(3, fName.length() - 4);
		if (parts.length == 3) {
			String userID = parts[0];
			String rating = parts[1];
			output.collect(new Text(Long.parseLong(fName) + "\t" + userID),
					new Text(rating));
		}

	}
}
