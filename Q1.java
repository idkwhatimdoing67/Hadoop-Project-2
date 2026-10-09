import java.io.IOException;

import javax.naming.Context;

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
        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();

        public void map(Object key, org.w3c.dom.Text value, Context context) throws IOException, InterruptedException {
            // Skip header, split by comma, filter for Region and valid temp

            String line = value.toString(); //read in the info

            // Skip header
            if (line.startsWith("Region") || line.trim().isEmpty()) {
                return;
            }

            // Split by comma
            String[] parts = line.split(",");
            if (parts.length >= 8) {
                String region = parts[0];
                String state = parts[2];
                String year = parts[6];
                String tempStr = parts[7];

                // Check region match
                if (region.equals("West")) {
                    try {
                        double temp = Double.parseDouble(tempStr);
                        
                        if (temp != 9999.9) {
                            outKey.set(state + "\t" + year);
                            outValue.set(temp);
                            context.write(outKey, outValue);
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
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