import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author sai Mapper for second mapred task to calculate similarity 
 * Reads user ratings and outputs ratings given by all users for a movie pair
 */
public class SimilarityMapper2 implements
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
		String[] lineParts = line.toString().split("\t");
		String[] userRatings = lineParts[1].split(";");
		for (int i = 0; i < userRatings.length; i++) {
			for (int j = 0; j < userRatings.length; j++) {
				String[] rating1 = userRatings[i].split(",");
				String[] rating2 = userRatings[j].split(",");
				output.collect(new Text(rating1[0] + "," + rating2[0]),
						new Text(rating1[1] + "," + rating2[1]));
			}
		}
	}

}
