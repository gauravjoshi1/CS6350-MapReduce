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

public class InvertedIndex {
    public static class Map extends Mapper <LongWritable, Text, Text, Text>{
        // keep a track of the current line number
        private int currLineNum = 1;

        // mapper emits key-value pairs of user-id of a person with tuple of person user_id and friend user_id
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] inputFile = value.toString().split(",");
            for (String currStr : inputFile) {
                String lineNumber = Integer.toString(currLineNum);
                Text currStrToText = new Text(currStr);
                Text lineText = new Text(lineNumber);

                context.write(currStrToText, lineText);
            }

            // the map function is called on each line
            // update the current line number before exiting map function
            currLineNum = currLineNum + 1;
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> listOfValues = new ArrayList<>();
            for (Text value: values) {
                listOfValues.add(value.toString());
            }

            Collections.sort(listOfValues);
            Text listOfValuesAsText = new Text(listOfValues.toString());
            context.write(key, listOfValuesAsText);
        }
    }

    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(Map.class);
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