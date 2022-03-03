import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class InMemoryMapper {
    public static class InMemoryMapperMap extends Mapper <LongWritable, Text, Text, Text>{
        private final Text user = new Text();
        Map<String, String> currMap = new HashMap<>();
        private final Text dateOfBirthsOfFriends = new Text(); //type of output key

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
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
                    currMap.put(currArr[0], currArr[9]);
                    currLine = bufferRead.readLine();
                }
            }
        }

        // mapper emits key-value pairs of user-id of a person with tuple of person user_id and friend user_id
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] inputFile = value.toString().split("\\t");
            if (inputFile.length < 2) return;

            String userStr = inputFile[0], friendList = inputFile[1];
            StringBuilder friendsBirthdayInfo = new StringBuilder();

            int userIdFromFile = Integer.parseInt(userStr);
            int[] friendsOfUserFromFile = Arrays.stream(friendList.split(",")).mapToInt(Integer::parseInt).toArray();

            for (int friendId : friendsOfUserFromFile) {
                friendsBirthdayInfo.append(currMap.get(String.valueOf(friendId))).append(",");
            }

            // remove trailing commas
            friendsBirthdayInfo.deleteCharAt(friendsBirthdayInfo.length() - 1);

            for (int friendId : friendsOfUserFromFile) {
                String userIdFriendIdTuple = "";
                if (userIdFromFile > friendId) userIdFriendIdTuple = friendId + "," + userIdFromFile;
                else userIdFriendIdTuple = userIdFromFile + "," + friendId;

                user.set(userIdFriendIdTuple);
                dateOfBirthsOfFriends.set(friendsBirthdayInfo.toString());
                context.write(user, dateOfBirthsOfFriends);
            }
        }
    }

    public static class InMemoryMapperReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Integer countOfFriendsBornAfter1995 = 0;
            Set<String> firstSetOfInputs = new HashSet<>();
            Set<String> secondSetOfInputs = new HashSet<>();
            int count = 0;

            for (Text value : values) {
                String currVal = value.toString();
                String[] currFriendList = currVal.split(",");
                if (count == 0) firstSetOfInputs = new HashSet<>(Arrays.asList(currFriendList));
                if (count == 1) secondSetOfInputs = new HashSet<>(Arrays.asList(currFriendList));

                count++;
            }

            firstSetOfInputs.retainAll(secondSetOfInputs);
            String dateOfBirthStr = firstSetOfInputs.toString();
            String[] listOfFriendDOB = dateOfBirthStr.split(",");
            StringBuilder formatFriendDOB = new StringBuilder();

            for (String friendDOB : listOfFriendDOB) {
                formatFriendDOB.append(friendDOB).append(", ");
                String[] birthInfo = friendDOB.split("/");

                try {
                    String refBirthDay = birthInfo[2].replaceAll("]", "");
                    int birthYear = Integer.parseInt(refBirthDay);
                    countOfFriendsBornAfter1995 = birthYear <= 1995 ? countOfFriendsBornAfter1995 : countOfFriendsBornAfter1995 + 1;
                }

                catch(Exception ignored){}
            }

            formatFriendDOB = new StringBuilder(formatFriendDOB.substring(0, formatFriendDOB.length() - 2)).append(", ").append(countOfFriendsBornAfter1995);
            context.write(key, new Text(formatFriendDOB.toString()));
        }
    }

    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("userdata.txt", args[1]);
        //create a job with name "wordcount"
        Job job = new Job(conf, "In Memory Mapper Side");
        job.setJarByClass(InMemoryMapper.class);
        job.setMapperClass(InMemoryMapperMap.class);
        job.setReducerClass(InMemoryMapperReduce.class);

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