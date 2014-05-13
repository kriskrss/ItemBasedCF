import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author sai Mapper to collect ratings of a given customer
 */
public class CustomerRatingsMapper implements
		Mapper<LongWritable, Text, Text, Text> {
	private String customerID;

	@Override
	public void configure(JobConf conf) {
		customerID = conf.get("CID");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void map(LongWritable key, Text line,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] parts = line.toString().split("\t");
		if (customerID.equals(parts[0])) {
			String[] ratings = parts[1].split(";");
			for (int i = 0; i < ratings.length; i++) {
				String[] rating = ratings[i].split(",");
				output.collect(new Text(rating[0]), new Text(rating[1]));
			}
		}

	}

}
