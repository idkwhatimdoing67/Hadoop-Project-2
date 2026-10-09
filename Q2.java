import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import javax.naming.Context;


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


public class Q2 {

    public static class JoinMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private HashMap<String, String> capitals = new Hashmap<>();
        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();


        @Override
        protected void setup(Context context) throws IOException {
            // Read state-capitals.csv and populate hashmap

            URI[] cacheFiles = context.getCacheFiles();
            if(cacheFiles != null && cacheFiles().length > 0) {
                // Get file name from cached URI
                String fileName = new File(cacheFiles[0].getPath()).getName();
                BufferedReader reader = new BufferedReader(new FileReader(fileName));

                String line;
                boolean header = true;
                while ((line = reader.readLine()) != null) {
                    if (header) { 
                        header = false; 
                        continue; 
                    }
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        capitals.put(parts[0], parts[1]);
                    }
                }
                reader.close();
            }

            
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header, validate temp, check if State and Citymatch a capital in hashmap
        }

    }

    public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterrupptedException {
            // Calculate 5-Year avg
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 3) {
            System.err.println("Usage: Q2 <temp_in> <capitals_cache> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Q2 Map Side Join");
        job.setJarByClass(Q2.class);
        
        // Add capitals file to distributed cache
        job.addCacheFile(new URI(otherArgs[1]));

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(AvgReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}