import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author sai Mapper for first mapred task to calculate similarity
 *  Groups ratings given by each user
 */
public class SimilarityMapper1 implements
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

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
		String movie = parts[0];
		String userid = parts[1];
		String rating = parts[2];
		output.collect(new Text(userid), new Text(movie + "," + rating));
	}

}
