import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxOccurence {
    public static class Map extends Mapper <LongWritable, Text, Text, Text>{

        // mapper emits key-value pairs of user-id of a person with tuple of person user_id and friend user_id
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] inputFile = value.toString().split("\\t");
            String currWord = inputFile[0];
            StringBuilder wordCount = new StringBuilder(inputFile[1]);

            // remove open bracket from output file
            wordCount.deleteCharAt(0);

            // remove closing bracket from output file
            wordCount.deleteCharAt(wordCount.length() - 1);

            // convert the word count into list of string
            String[] countOfWords = wordCount.toString().split(",");
            String lengthOfWords = String.valueOf(countOfWords.length);

            context.write(new Text(currWord), new Text(lengthOfWords));
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        private int maxCount = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values) {
                int currValue = Integer.parseInt(value.toString());

                if (currValue >= maxCount) {
                    maxCount = currValue;
                    context.write(key, new Text(String.valueOf(maxCount)));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private int maxCount = 0;
        List<String> maxOccurringWords = new ArrayList<>();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                int currValue = Integer.parseInt(value.toString());

                if (currValue < maxCount) continue;
                if (currValue == maxCount) {
                    maxOccurringWords.add(key.toString());
                }

                if (currValue > maxCount) {
                    maxOccurringWords.clear();
                    maxCount = currValue;
                    maxOccurringWords.add(key.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(maxOccurringWords.toString()), new Text(String.valueOf(maxCount)));
            super.cleanup(context);
        }
    }

    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MaxOccurrence");
        job.setJarByClass(MaxOccurence.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);

        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}