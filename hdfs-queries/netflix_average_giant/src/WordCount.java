import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    public static class RatingMapper
            extends Mapper<Object, Text, Text, LongWritable>{

            private Text text = new Text();
            private LongWritable ratingOut = new LongWritable();

            @Override
            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {

                String line = value.toString();
                String[] parts = line.split(",");
                int rating = Integer.parseInt(parts[2]);

                text.set("ratings");
                ratingOut.set(rating);
                context.write(text, ratingOut);

                text.set("count");
                ratingOut.set(1);
                context.write(text, ratingOut);
            }
    }

    public static class SumCombiner
            extends Reducer<Text,LongWritable,Text,LongWritable> {

            private Text text = new Text();
            private LongWritable sumOut = new LongWritable();

            @Override
            public void reduce(Text key, Iterable<LongWritable> values, Context context) 
            throws IOException, InterruptedException {

                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                sumOut.set(sum);
                context.write(key, sumOut);
            }
    }

    public static void main(String[] args) throws Exception {
        for(int i = 1; i <= 1; i++) {
            for (int j = 1; j <= 10; j++) {
                Configuration conf = new Configuration();

                Job covarianceJob = Job.getInstance(conf, "netflix average giant: " + i + ", " + j);
                covarianceJob.setJarByClass(WordCount.class);

                covarianceJob.setMapperClass(RatingMapper.class);
                covarianceJob.setCombinerClass(SumCombiner.class);
                covarianceJob.setReducerClass(SumCombiner.class);

                covarianceJob.setMapOutputKeyClass(Text.class);
                covarianceJob.setMapOutputValueClass(LongWritable.class);

                covarianceJob.setOutputKeyClass(Text.class);
                covarianceJob.setOutputValueClass(LongWritable.class);

                covarianceJob.setInputFormatClass(TextInputFormat.class);
                covarianceJob.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(covarianceJob, new Path(args[0]));
                FileOutputFormat.setOutputPath(covarianceJob, new Path(args[1] + "/" + i + "/" + j));

                covarianceJob.waitForCompletion(true);
            }
        }
        System.exit(0);
    }
}
