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
            extends Mapper<Object, Text, IntWritable, IntWritable>{

            private IntWritable movieOut = new IntWritable();
            private IntWritable ratingOut = new IntWritable();

            @Override
            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {

                String line = value.toString();
                String[] parts = line.split(",");
                int movieId = Integer.parseInt(parts[0]);
                int rating = Integer.parseInt(parts[2]);
                if (movieId <= 1000) {
                    movieOut.set(movieId);
                    ratingOut.set(rating);
                    context.write(movieOut, ratingOut);
                }
            }

    }

    public static class AverageReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,DoubleWritable> {

            private IntWritable movieOut = new IntWritable();
            private DoubleWritable avgOut = new DoubleWritable();

            @Override
            public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {

                long sum = 0;
                long count = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                    count++;
                }
                double avg = (sum * 1.0) / count;
                avgOut.set(avg);
                context.write(key, avgOut);
            }

    }

    public static void main(String[] args) throws Exception {
        for (int i = 1; i <= 5; i++) {
            Configuration conf = new Configuration();

            Job covarianceJob = Job.getInstance(conf, "multiple averages job " + i);
            covarianceJob.setJarByClass(WordCount.class);

            covarianceJob.setMapperClass(RatingMapper.class);
            covarianceJob.setReducerClass(AverageReducer.class);

            covarianceJob.setMapOutputKeyClass(IntWritable.class);
            covarianceJob.setMapOutputValueClass(IntWritable.class);

            covarianceJob.setOutputKeyClass(IntWritable.class);
            covarianceJob.setOutputValueClass(DoubleWritable.class);

            covarianceJob.setInputFormatClass(TextInputFormat.class);
            covarianceJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(covarianceJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(covarianceJob, new Path(args[1] + "/" + i));

            covarianceJob.waitForCompletion(true);
        }

        System.exit(0);
    }
}
