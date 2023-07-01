import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataClean {

    public static class DataCleanMapper
            extends Mapper<Object, Text, IntWritable, Text>{

        private int counter = 1;
        private String prev_value = "";
        private IntWritable output_key = new IntWritable();
        private Text output_val = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] key_val_pair = itr.nextToken().split(",");

                int temp_key = Integer.parseInt(key_val_pair[0]);
                String temp_val = key_val_pair[1];

                if (counter == temp_key) {
                    output_key.set(temp_key);
                    output_val.set(temp_val);
                    context.write(output_key, output_val);

                }
                else {
                    output_key.set(counter - 1);
                    output_val.set(prev_value);
                    context.write(output_key, output_val);

                    output_key.set(temp_key);
                    output_val.set(temp_val);
                    context.write(output_key, output_val);

                    counter += 1;
                }

                counter += 1;
                prev_value = temp_val;

            }
        }
    }

    public static class DataCleanReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        private int counter;
        private IntWritable output_key = new IntWritable();
        private Text output_val = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            counter = key.get();
            for (Text val : values) {
                output_key.set(counter);
                output_val.set(val);
                context.write(output_key, output_val);
                counter += 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "data clean");
        job.setJarByClass(DataClean.class);
        job.setMapperClass(DataCleanMapper.class);
        job.setCombinerClass(DataCleanReducer.class);
        job.setReducerClass(DataCleanReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}