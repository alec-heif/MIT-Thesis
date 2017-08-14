import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

    public static class HashMapper
            extends Mapper<Object, Text, LongWritable, LongWritable>{

            private LongWritable num = new LongWritable();
            private final static LongWritable one = new LongWritable(1);

            @Override
            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {

                String line = value.toString();
                long hashWidth = 1000000;
                long output = Math.abs(line.hashCode()) % hashWidth;
                num.set(output);
                context.write(num, one);
            }
    }

    public static class LongSumCombiner 
            extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {

            private LongWritable result = new LongWritable();

            @Override
            public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
                throws IOException, InterruptedException {

                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
    }

    public static class MedianReducer
            extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {

            private long numItems;
            private long seenItems = 0;
            private boolean medianFound = false;
            private LongWritable num = new LongWritable();

            @Override
            public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
                throws IOException, InterruptedException {

                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                seenItems += sum;

                if (!medianFound && seenItems >= numItems / 2) {
                    medianFound = true;
                    num.set(seenItems);
                    context.write(key, num);
                }
            }

            @Override
            public void setup(Context ctx) throws IOException, InterruptedException {
                Configuration conf = ctx.getConfiguration();
                Cluster cluster = new Cluster(conf);
                Job currentJob = cluster.getJob(ctx.getJobID());
                numItems = currentJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
            }
    }

    public static void main(String[] args) throws Exception {
        for (int i = 1; i < 6; i++) {
            Configuration conf = new Configuration();

            Job hashJob = Job.getInstance(conf, "hash job " + i);
            hashJob.setJarByClass(WordCount.class);

            hashJob.setMapperClass(HashMapper.class);
            hashJob.setCombinerClass(LongSumCombiner.class);
            hashJob.setReducerClass(MedianReducer.class);

            hashJob.setOutputKeyClass(LongWritable.class);
            hashJob.setOutputValueClass(LongWritable.class);

            hashJob.setInputFormatClass(TextInputFormat.class);
            hashJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(hashJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(hashJob, new Path(args[1] + "_" + i));

            hashJob.setNumReduceTasks(1);

            hashJob.waitForCompletion(true);
        }

        System.exit(0);
    }
}
