import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author sai Mapper to group similarities for a given movie
 */
public class MovieSimilarityMapper implements
		Mapper<LongWritable, Text, Text, Text> {
	private Long movie;

	@Override
	public void configure(JobConf conf) {
		movie = Long.parseLong(conf.get("movie"));
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
		String[] moviePair = parts[0].split(",");
		if (Long.parseLong(moviePair[0]) == movie) {
			output.collect(new Text(moviePair[1]), new Text(parts[1]));
		} else if (moviePair[1].equals(movie)) {
			output.collect(new Text(moviePair[0]), new Text(parts[1]));
		}

	}

}
