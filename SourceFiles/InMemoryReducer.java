import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InMemoryReducer {
    public static class InMemoryReducerMap extends Mapper <LongWritable, Text, Text, Text>{

        // mapper emits key-value pairs of user-id of a person with tuple of person user_id and friend user_id
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] inputFile = value.toString().split("\\t");
            if (inputFile.length < 2) return;

            context.write(new Text(inputFile[0]), new Text(inputFile[1]));
        }
    }

    public static class InMemoryReducerReduce extends Reducer<Text, Text, Text, Text> {
        Map<String, Integer> mapOfFriendsWithAge = new HashMap<>();

        @Override
        public void setup(Reducer.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path path = new Path(conf.get("userdata.txt"));
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(path);

            for (FileStatus currStatus : status){
                Path filePath = currStatus.getPath();
                InputStreamReader read = new InputStreamReader(fs.open(filePath));
                BufferedReader bufferRead = new BufferedReader(read);
                String currLine = bufferRead.readLine();

                while (currLine != null) {
                    String[] currArr = currLine.split(",");
                    // key of the map is line and value is date of births
                    mapOfFriendsWithAge.put(currArr[0], ageOfFriend(currArr[9]));
                    currLine = bufferRead.readLine();
                }
            }
        }

        private int ageOfFriend(String dateOfBirth) {
            LocalDate currentDate = LocalDate.now();
            String[] divideIntoDayMonthYear = dateOfBirth.split("/");
            int month = Integer.parseInt(divideIntoDayMonthYear[0]);
            int day = Integer.parseInt(divideIntoDayMonthYear[1]);
            int year = Integer.parseInt(divideIntoDayMonthYear[2]);

            LocalDate dateOfBirthInDateFormat = LocalDate.of(year, month, day);

            return Period.between(dateOfBirthInDateFormat, currentDate).getYears();
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                String[] listOfFriends = value.toString().split(",");
                // initialize the min age pointer to max value
                // keep finding min value between age of friends and min age
                int minimumAge = Integer.MAX_VALUE;

                for (String friend : listOfFriends) {
                    minimumAge = Math.min(minimumAge, mapOfFriendsWithAge.get(friend));
                }

                context.write(key, new Text(String.valueOf(minimumAge)));
            }
        }
    }

    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("userdata.txt", args[1]);
        //create a job with name "wordcount"
        Job job = new Job(conf, "In Memory Reducer Side");
        job.setJarByClass(InMemoryReducer.class);
        job.setMapperClass(InMemoryReducerMap.class);
        job.setReducerClass(InMemoryReducerReduce.class);

        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}