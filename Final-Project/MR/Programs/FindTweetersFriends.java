import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


//do inmapper combiner
public class FindTweetersFriends {
	
	public static class TweetFriendMapper 
	extends Mapper<LongWritable, Text, Text, Text>{

		private HashMap<Integer, Integer> combiner = new HashMap<Integer, Integer>();
		// Finds all tags based on the @ symbol and returns an ArrayList of it
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            try{
                BufferedReader readBuffer = new BufferedReader(new FileReader(uris[0].toString()));
                String line = "";
                while ((line=readBuffer.readLine())!=null){
                	String parts[] = line.split("\t");
                    combiner.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                }
                readBuffer.close(); 
            }       
            catch (Exception e){
                System.out.println(e.toString());
            }
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			if (value.toString().trim().length() != 0) {
        	String parts[] = value.toString().replaceAll("\\s+", " ").split(" ");
        	if (parts.length >= 5) {
        		// making sure we have everything
        		if (NumberUtils.isNumber(parts[0]) &&
        				NumberUtils.isNumber(parts[2]) && 
        				NumberUtils.isNumber(parts[3]) &&
        				NumberUtils.isNumber(parts[4]) &&
        				NumberUtils.isNumber(parts[5])) {
        			int user = Integer.parseInt(parts[0]);
               
                	if (combiner.containsKey(user)) {
                		int followers = Integer.parseInt(parts[3]);
                		//context.write(new Text(Integer.toString(user)), 
                			//	new TweetsAndFollowers(combiner.get(user), followers));
                		context.write(new Text(Integer.toString(user)), new Text(Integer.toString(combiner.get(user)) + 
                				" " + Integer.toString(followers)));
                	}
        		}
        	}
			}
		}
	}

	public static class TweeterReducer extends Reducer<Text,Text,Text,Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			int total_tweets = 0;
			int total_followers = 0;
			boolean invalid = false;
			Iterator<Text> it = values.iterator();
			Text tnf;
			while (it.hasNext()) {
				tnf = it.next();
				
				//total_tweets += tnf.tweets;
				String users_followers = tnf.toString().replaceFirst("\\s+", " ");
				String split[] = users_followers.split(" ");
				if (split.length < 2) {
					System.out.println(users_followers);
					invalid = true;
				}
				else {total_tweets += Integer.parseInt(split[0]);
					total_followers += Integer.parseInt(split[1]);
				}
			}
			if (!invalid)
				context.write(key, new Text(Integer.toString(total_tweets) + "\t" + Integer.toString(total_followers)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: TweetCountPerUser <in> <out> <tweeters> <num_reducers>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(FindTweetersFriends.class);
		job.setMapperClass(FindTweetersFriends.TweetFriendMapper.class);
		job.setCombinerClass(FindTweetersFriends.TweeterReducer.class);
		job.setReducerClass(FindTweetersFriends.TweeterReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		DistributedCache.addCacheFile(new URI(otherArgs[2]), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}