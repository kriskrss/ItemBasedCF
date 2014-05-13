import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RecommendationsDriver extends Configured implements Tool {
	private HashSet<Long> unratedMovies;
	private HashMap<Long, Double> ratedMovies = new HashMap<Long, Double>();
	private HashMap<Long, Double> calculatedRatings = new HashMap<Long, Double>();
	private LinkedHashMap<Long, Double> sortedRatings;

	@Override
	public int run(String[] args) throws Exception {
		// Get ratings of a given customer
		JobConf customerRatingsConf = new JobConf(getConf(),
				RecommendationsDriver.class);
		customerRatingsConf.setJobName("Customer Ratings");
		customerRatingsConf.setOutputKeyClass(Text.class);
		customerRatingsConf.set("CID", args[1]);
		customerRatingsConf.setJarByClass(ItemBasedCFDriver.class);
		customerRatingsConf.setMapperClass(CustomerRatingsMapper.class);
		customerRatingsConf.setReducerClass(DefaultReducer.class);
		FileInputFormat.addInputPath(customerRatingsConf, new Path(args[0]
				+ "/UserRatings"));
		FileOutputFormat.setOutputPath(customerRatingsConf, new Path(args[0]
				+ "/CustomerRatings/" + args[1]));
		JobClient.runJob(customerRatingsConf);
		// Get all unrated movies for a given customer
		getUnratedMovies(args[0], args[1], Long.parseLong(args[2]));
		Iterator<Long> unrated = unratedMovies.iterator();
		while (unrated.hasNext()) {
			Long mid = unrated.next();
			// Get movies similar to a given movie
			JobConf movieSimilarityConf = new JobConf(getConf(),
					RecommendationsDriver.class);
			movieSimilarityConf.setJobName("Similar Movies");
			movieSimilarityConf.set("movie", "" + mid);
			movieSimilarityConf.setOutputKeyClass(Text.class);
			movieSimilarityConf.setJarByClass(ItemBasedCFDriver.class);
			movieSimilarityConf.setMapperClass(MovieSimilarityMapper.class);
			movieSimilarityConf.setReducerClass(DefaultReducer.class);
			FileInputFormat.addInputPath(movieSimilarityConf, new Path(args[0]
					+ "/Similarity"));
			FileOutputFormat.setOutputPath(movieSimilarityConf, new Path(
					args[0] + "/SimilarMovies/" + mid));
			JobClient.runJob(movieSimilarityConf);
			// calculate rating of given movie based on user ratings for similar
			// movies
			calculateRating(args[0], mid);
		}
		// Club calculated ratings to already known ratings
		Iterator<Long> cit = calculatedRatings.keySet().iterator();
		while (cit.hasNext()) {
			Long cur = cit.next();
			ratedMovies.put(cur, calculatedRatings.get(cur));
		}
		// Sort all rated movies
		sortedRatings = sortHashMapByValuesD(ratedMovies);
		// Print recommendations
		printRatings(Integer.parseInt(args[3]));
		// Store ratings in a file
		writeRatingsToFile(args[1], args[0]);
		return 0;
	}

	private void writeRatingsToFile(String cid, String output) {
		try {
			Configuration configuration = new Configuration();
			FileSystem hdfs = FileSystem.get(configuration);
			Path file = new Path(output + "/CustomerRatings/" + cid
					+ "/updatedratings");
			if (hdfs.exists(file)) {
				hdfs.delete(file, true);
			}
			OutputStream os = hdfs.create(file);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
					"UTF-8"));
			Iterator<Long> it = sortedRatings.keySet().iterator();
			while (it.hasNext()) {
				Long curMovie = it.next();
				br.write("MovieID: " + curMovie + " Rating: "
						+ sortedRatings.get(curMovie));
				if (it.hasNext()) {
					br.write("\n");
				}
			}
			br.flush();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public LinkedHashMap<Long, Double> sortHashMapByValuesD(
			HashMap<Long, Double> passedMap) {
		List<Long> mapKeys = new ArrayList<Long>(passedMap.keySet());
		List<Double> mapValues = new ArrayList<Double>(passedMap.values());
		Collections.sort(mapValues, new Comparator<Double>() {

			@Override
			public int compare(Double o1, Double o2) {
				if (o1 < o2) {
					return 1;
				} else if (o1 > o2) {
					return -1;
				} else {
					return 0;
				}
			}
		});
		Collections.sort(mapKeys, new Comparator<Long>() {

			@Override
			public int compare(Long o1, Long o2) {
				if (o1 < o2) {
					return 1;
				} else if (o1 > o2) {
					return -1;
				} else {
					return 0;
				}
			}
		});

		LinkedHashMap<Long, Double> sortedMap = new LinkedHashMap<Long, Double>();

		Iterator<Double> valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			double val = valueIt.next();
			Iterator<Long> keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				long key = keyIt.next();
				double comp1 = passedMap.get(key);
				double comp2 = val;

				if (comp1 == comp2) {
					passedMap.remove(key);
					mapKeys.remove(key);
					sortedMap.put(key, val);
					break;
				}

			}

		}
		return sortedMap;
	}

	private void printRatings(int i) {
		Iterator<Long> it = sortedRatings.keySet().iterator();
		while (it.hasNext() && i > 0) {
			Long curMovie = it.next();
			System.out.println("MovieID: " + curMovie + " Rating: "
					+ sortedRatings.get(curMovie));
			i--;
		}
	}

	private void calculateRating(String output, Long mid) {
		Path path = new Path(output + "/SimilarMovies/" + mid + "/part-00000");
		HashMap<Long, Double> similarMovies = new HashMap<Long, Double>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line = br.readLine();
			while (line != null) {
				String[] similarity = line.split("\t");
				similarMovies.put(Long.parseLong(similarity[0]),
						Double.parseDouble(similarity[1]));
				line = br.readLine();
			}
			Iterator<Long> it = similarMovies.keySet().iterator();
			double rating = 0;
			double count = 0;
			while (it.hasNext()) {
				Long curMovie = it.next();
				if (ratedMovies.containsKey(curMovie)) {
					rating += similarMovies.get(curMovie)
							* ratedMovies.get(curMovie);
					count = count + 1;
				}
			}
			rating = rating / count;
			calculatedRatings.put(mid, rating);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void getUnratedMovies(String output, String CID, long numMovies) {
		Path path = new Path(output + "/CustomerRatings/" + CID + "/part-00000");
		unratedMovies = new HashSet<Long>();
		ratedMovies = new HashMap<Long, Double>();
		for (long i = 1; i <= numMovies; i++) {
			unratedMovies.add(i);
		}
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line = br.readLine();
			while (line != null) {
				String[] rating = line.split("\t");
				unratedMovies.remove(Long.parseLong(rating[0]));
				ratedMovies.put(Long.parseLong(rating[0]),
						Double.parseDouble(rating[1]));
				line = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new RecommendationsDriver(), args);
		System.exit(res);
	}

}
