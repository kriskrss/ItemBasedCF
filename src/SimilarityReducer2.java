import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/**
 * 
 * @author sai
 * Calculate similarity(using correlation) for each movie pair
 */
public class SimilarityReducer2 implements Reducer<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(Text moviePair, Iterator<Text> ratings,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		ArrayList<Double> ratings1 = new ArrayList<Double>();
		ArrayList<Double> ratings2 = new ArrayList<Double>();
		double numRatings = 0;
		double totRating1 = 0, totrating2 = 0;
		while (ratings.hasNext()) {
			String[] rating = ratings.next().toString().split(",");
			ratings1.add(Double.parseDouble(rating[0]));
			ratings2.add(Double.parseDouble(rating[1]));
			totRating1 += Double.parseDouble(rating[0]);
			totrating2 += Double.parseDouble(rating[1]);
			numRatings += 1;
		}
		double avgRating1 = totRating1 / numRatings;
		double avgRating2 = totrating2 / numRatings;
		double sum1 = 0.0, sum2 = 0.0, sumProduct = 0.0;
		for (int i = 0; i < numRatings; i++) {
			sum1 += Math.pow((ratings1.get(i) - avgRating1), 2);
			sum2 += Math.pow((ratings2.get(i) - avgRating2), 2);
			sumProduct += (ratings1.get(i) - avgRating1)
					* (ratings2.get(i) - avgRating2);
		}
		double corr = sumProduct / (Math.sqrt(sum1) * Math.sqrt(sum2));
		System.out.println(sum1 + "," + sum2 + "," + sumProduct + "," + corr);
		if (Double.isNaN(corr)) {
			corr = 0;
		}
		output.collect(moviePair, new Text(corr + ""));
	}
}
