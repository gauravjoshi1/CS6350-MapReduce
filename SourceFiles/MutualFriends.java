import java.io.IOException;
import java.util.*;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {
    public static class Map extends Mapper <LongWritable, Text, Text, Text>{
        private final Text user = new Text();
        private final Text friendsOfUser = new Text(); //type of output key

        // mapper emits key-value pairs of user-id of a person with tuple of person user_id and friend user_id
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] inputFile = value.toString().split("\\t");
            if (inputFile.length < 2) return;

            String userStr = inputFile[0], friendList = inputFile[1];
            int userIdFromFile = Integer.parseInt(userStr);
            int[] friendsOfUserFromFile = Arrays.stream(friendList.split(",")).mapToInt(Integer::parseInt).toArray();

            for (int friendId : friendsOfUserFromFile) {
                String userIdFriendIdTuple = "";
                if (userIdFromFile > friendId) userIdFriendIdTuple = friendId + "," + userIdFromFile;
                else userIdFriendIdTuple = userIdFromFile + "," + friendId;

                user.set(userIdFriendIdTuple);
                friendsOfUser.set(inputFile[1]);
                context.write(user, friendsOfUser);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> firstSetOfInputs = new HashSet<>();
            Set<String> secondSetOfInputs = new HashSet<>();
            boolean isNull = false;
            int count = 0;

            for (Text value : values) {
                String currVal = value.toString();
                if (currVal == null) {
                    isNull = true;
                    break;
                }

                String[] currFriendList = currVal.split(",");
                if (count == 0) firstSetOfInputs = new HashSet<>(Arrays.asList(currFriendList));
                if (count == 1) secondSetOfInputs = new HashSet<>(Arrays.asList(currFriendList));

                count++;
            }

            if (!isNull) {
                firstSetOfInputs.retainAll(secondSetOfInputs);
                String mutualFriends = firstSetOfInputs.toString();
                context.write(key, new Text(mutualFriends));
            }
        }
    }

    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //create a job with name "wordcount"
        Job job = new Job(conf, "Mutual Friends");
        job.setJarByClass(MutualFriends.class);
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