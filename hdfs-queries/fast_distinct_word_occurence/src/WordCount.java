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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    public static class DistinctMapper
            extends Mapper<Object, Text, Text, NullWritable>{

            private Text word = new Text();

            @Override
            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {

                String query = value.toString().split("\t")[1];
                if (query.contains("mac") || query.contains("pc")) {
                    word.set(query);
                    context.write(word, NullWritable.get());
                }
            }
    }

    public static class DistinctReducer
            extends Reducer<Text,NullWritable,Text,NullWritable> {

            @Override
            public void reduce(Text key, Iterable<NullWritable> values, Context context) 
                throws IOException, InterruptedException {

                context.write(key, NullWritable.get());

            }
    }

    public static class WordCountMapper
            extends Mapper<Object, Text, Text, LongWritable>{

            private Text word = new Text();
            private final static LongWritable one = new LongWritable(1);

            @Override
            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {

                String[] split = value.toString().split(" ");
                for (String x : split) {
                    if (x.equals("mac") || x.equals("pc")) {
                        word.set(x);
                        context.write(word, one);
                    }
                }
            }
    }

    public static class WordCountReducer
            extends Reducer<Text,LongWritable,Text,LongWritable> {
            private LongWritable result = new LongWritable();

            @Override
            public void reduce(Text key, Iterable<LongWritable> values, Context context) 
                throws IOException, InterruptedException {

                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);

            }
    }

    public static void main(String[] args) throws Exception {

        for (int i = 1; i <= 4; i++) {
            Configuration conf = new Configuration();
            Job distinctJob = Job.getInstance(conf, "distinct word");
            distinctJob.setJarByClass(WordCount.class);

            distinctJob.setMapperClass(DistinctMapper.class);
            distinctJob.setReducerClass(DistinctReducer.class);

            distinctJob.setOutputKeyClass(Text.class);
            distinctJob.setOutputValueClass(NullWritable.class);

            distinctJob.setInputFormatClass(TextInputFormat.class);
            distinctJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(distinctJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(distinctJob, new Path(args[1] + "_" + i));

            distinctJob.waitForCompletion(true);

            Job countJob = Job.getInstance(conf, "word count");
            countJob.setJarByClass(WordCount.class);

            countJob.setMapperClass(WordCountMapper.class);
            countJob.setReducerClass(WordCountReducer.class);

            countJob.setOutputKeyClass(Text.class);
            countJob.setOutputValueClass(LongWritable.class);

            countJob.setInputFormatClass(TextInputFormat.class);
            countJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(countJob, new Path(args[1] + "_" + i));
            FileOutputFormat.setOutputPath(countJob, new Path(args[2] + "_" + i));

            countJob.waitForCompletion(true);
        }

        System.exit(0);
    }
}
