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

public class Q3 {

    public static class TempMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Region") || line.trim().isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 8) {
                String state = parts[2];
                String city = parts[3];
                String tempStr = parts[7];

                try {
                    double temp = Double.parseDouble(tempStr);
                    if (temp != 9999.9) {
                        outKey.set(state);
                        outValue.set("TEMP," + city + "," + temp);
                        context.write(outKey, outValue);
                    }
                } catch (NumberFormatException e) {
                }
            }
        }
    }

    public static class CapMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("State") || line.trim().isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 2) {
                String state = parts[0];
                String capital = parts[1];
                
                outKey.set(state);
                outValue.set("CAP," + capital);
                context.write(outKey, outValue);
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String capital = null;
            // Map to hold sum and count for each city
            HashMap<String, double[]> cityStats = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                
                if (parts[0].equals("CAP")) {
                    capital = parts[1];
                } else if (parts[0].equals("TEMP")) {
                    String city = parts[1];
                    double temp = Double.parseDouble(parts[2]);
                    
                    cityStats.putIfAbsent(city, new double[]{0.0, 0.0});
                    cityStats.get(city)[0] += temp; // sum
                    cityStats.get(city)[1] += 1.0;  // count
                }
            }

            // If cap found for state && we have the info
            if (capital != null && cityStats.containsKey(capital)) {
                double[] stats = cityStats.get(capital);
                double avg = stats[0] / stats[1];
                
                // key is state, emit key
                context.write(new Text(key.toString() + "\t" + capital), new Text(String.format("%.2f", avg)));
            }
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

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TempMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, CapMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}