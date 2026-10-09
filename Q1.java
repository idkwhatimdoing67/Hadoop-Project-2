import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1 {
    public static class TempMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header, split by comma, filter for Region and valid temp
        }
    }

    public static class TempReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // Sum valid readings, count, get avg, output
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: Q1 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Q1 Average Temperature West");
        job.setJarByClass(Q1.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}