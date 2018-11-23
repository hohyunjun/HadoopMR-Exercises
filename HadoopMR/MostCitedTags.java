import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class MostCitedTags {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1 : Tag들의 숫자를 센다
        Job job1 = Job.getInstance(conf, "Most Cited Tags Counting");

        // Use the MostCitedTags.class file to point to the job jar
        job1.setJarByClass(MostCitedTags.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(TagsCounterMap.class);
        job1.setReducerClass(TagsCounterReduce.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Setting the input and output locations
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        // Submit the job and wait for it's completion
        job1.waitForCompletion(true);

        // Job2 : Tag들을 언급된 횟수에 따라 Sorting
        Job job2 = Job.getInstance(conf, "Sort By Cited Count Value");

        job2.setNumReduceTasks(1);

        job2.setJarByClass(MostCitedTags.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(SortByValueMap.class);
        job2.setReducerClass(SortByValueReduce.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Setting the input and output locations
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.waitForCompletion(true);
    }

    public static class TagsCounterMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replaceAll("\\p{Z}","");
            String[] fields = line.split(",");
            String tagField = fields[fields.length - 2];

            word.set(tagField);
            context.write(word, one);
        }
    }

    public static class TagsCounterReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            // Sum all the occurrences of the word (key)
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class SortByValueMap extends Mapper<Text, Text, IntWritable, Text> {
        private Text word = new Text();
        IntWritable frequency = new IntWritable();

        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            frequency.set(Integer.parseInt(value.toString()));
            context.write(frequency, key);
        }
    }

    public static class SortByValueReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }
}
