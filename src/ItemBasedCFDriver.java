import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ItemBasedCFDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// Job Conf for reading data and bringing it to standard format
		JobConf dataFormatConf = new JobConf(getConf(), ItemBasedCFDriver.class);
		dataFormatConf.setJobName("DataFormat");
		dataFormatConf.setOutputKeyClass(Text.class);
		dataFormatConf.setJarByClass(ItemBasedCFDriver.class);
		dataFormatConf.setMapperClass(DataFormatMapper.class);
		dataFormatConf.setReducerClass(DataFormatReducer.class);
		dataFormatConf.setInputFormat(DocumentInputFormat.class);
		FileInputFormat.addInputPath(dataFormatConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(dataFormatConf, new Path(args[1]
				+ "/FormattedRatings"));
		JobClient.runJob(dataFormatConf);
		// First mapred task to calculate movie similarities
		JobConf similarityConf1 = new JobConf(getConf(),
				ItemBasedCFDriver.class);
		similarityConf1.setJobName("Similarity Part 1");
		similarityConf1.setOutputKeyClass(Text.class);
		similarityConf1.setJarByClass(ItemBasedCFDriver.class);
		similarityConf1.setMapperClass(SimilarityMapper1.class);
		similarityConf1.setReducerClass(SimilarityReducer1.class);
		FileInputFormat.addInputPath(similarityConf1, new Path(args[1]
				+ "/FormattedRatings"));
		FileOutputFormat.setOutputPath(similarityConf1, new Path(args[1]
				+ "/UserRatings"));
		// Second mapred task to calculate movie similarities
		JobClient.runJob(similarityConf1);
		JobConf similarityConf2 = new JobConf(getConf(),
				ItemBasedCFDriver.class);
		similarityConf2.setJobName("Similarity Part 2");
		similarityConf2.setOutputKeyClass(Text.class);
		similarityConf2.setJarByClass(ItemBasedCFDriver.class);
		similarityConf2.setMapperClass(SimilarityMapper2.class);
		similarityConf2.setReducerClass(SimilarityReducer2.class);
		FileInputFormat.addInputPath(similarityConf2, new Path(args[1]
				+ "/UserRatings"));
		FileOutputFormat.setOutputPath(similarityConf2, new Path(args[1]
				+ "/Similarity"));
		JobClient.runJob(similarityConf2);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ItemBasedCFDriver(),
				args);
		System.exit(res);
	}

}
