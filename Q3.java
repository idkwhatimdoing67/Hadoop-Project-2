import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3{

    public static class TempMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header, filter valid temp
            // Emit state as key and ""TEMP, City, AvgTemp" as value
        }
    }

    public static class CapMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header, emit State as key and "CAP, Capital" as value
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Loop thru values, find Capital, sum temps by city, emit if cap has valid readings
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 3) {
            System.err.println("Usage: Q3 <temp_in> <capitals_in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Q3 Reduce Side Join");
        job.setJarByClass(Q3.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure MultipleInputs for the two different mappers
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TempMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, CapMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}