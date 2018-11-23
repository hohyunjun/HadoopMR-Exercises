import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MovieIdByTags {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
        MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);

        Job job = Job.getInstance(conf, "TagsAggregator");

        job.setJarByClass(MovieIdByTags.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BSONWritable.class);

        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<ObjectId, BSONObject, Text, Text> {
        private final Text tagOutput = new Text();
        private final Text movieIdOutput = new Text();

        public void map(ObjectId key, BSONObject value, Context context) throws IOException, InterruptedException {
            String tag = value.get("tag").toString();
            String movieId = value.get("movieId").toString();

            tagOutput.set(tag);
            movieIdOutput.set(movieId);
            context.write(tagOutput, movieIdOutput);
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        private final Text combinerOutput = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> ids = new HashSet<>(); 
            for (Text value : values) {
                ids.add(value.toString());
            }

            Iterator itr = ids.iterator();
            while (itr.hasNext()) {
                combinerOutput.set(itr.next().toString());
                context.write(key, combinerOutput);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, BSONWritable> {
        private BSONWritable reduceResult = new BSONWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            BasicBSONObject output = new BasicBSONObject();
            HashSet<String> ids = new HashSet<>(); 
            StringBuilder sb = new StringBuilder();

            for (Text value : values) {
                ids.add(value.toString());
            }
            
            Iterator itr = ids.iterator();
            while (itr.hasNext()) {
                sb.append(itr.next().toString());
                sb.append(", ");
            }

            String movieIds = sb.toString();
            if (movieIds.endsWith(", ")) {
                movieIds = movieIds.substring(0, movieIds.length() - 2);
            }
            
            output.put("moiveIds", movieIds);
            reduceResult.setDoc(output);
            context.write(key, reduceResult);
        }
    }
}
