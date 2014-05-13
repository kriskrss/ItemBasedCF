import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/**
 * 
 * @author sai
 * Collect all ratings for each user and store them in a file
 * One line for each user's ratings
 */
public class SimilarityReducer1 implements Reducer<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(Text userid, Iterator<Text> movieRatings,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		StringBuffer ratings = new StringBuffer();
		while (movieRatings.hasNext()) {
			ratings.append(movieRatings.next());
			if (movieRatings.hasNext()) {
				ratings.append(";");
			}
		}
		output.collect(userid, new Text(ratings.toString()));
	}
}
