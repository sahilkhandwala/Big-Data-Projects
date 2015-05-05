
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RetweetFriendCount {
	
	public static class RFriendCountMapper extends Mapper<Object, Text, IntWritable, IntWritable>
	{
		private HashMap<Integer, ArrayList<Integer>> retweetInfo = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

		protected void setup(Context context) throws IOException
		{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (files == null) {
				throw new RuntimeException(
						"User information is not set in DistributedCache");
			}
			for (int i = 0; i != files.length; i++) {
			BufferedReader br = new BufferedReader(new FileReader(files[i].toString()));
			String line;
			while ((line=br.readLine())!=null) {
				String[] s = line.split("\t");
				int k = Integer.parseInt(s[0]);
				int v = Integer.parseInt(s[1]);
				
				if(retweetInfo.containsKey(k))
				{
					ArrayList<Integer> tempList = retweetInfo.get(k);
					tempList.add(v);
					retweetInfo.put(k, tempList);
				}
				else
				{
					ArrayList<Integer> list = new ArrayList<Integer>();
					list.add(v);
					retweetInfo.put(k, list);
				}
			}
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Scanner s = new Scanner(value.toString()).useDelimiter("\t");
			int v = s.nextInt();
			int k = s.nextInt();
		
			
			if(retweetInfo.containsKey(k))
			{	// list of retweeters
				ArrayList<Integer> temp = retweetInfo.get(k);
				if(temp.contains(v))
				{	
					Iterator<Integer> temp_it = temp.iterator();
					int count_of_retweeter = 0;
					while(temp_it.hasNext()) {
						int user = temp_it.next();
						if (user == v)
							count_of_retweeter++;
					}
					if(map.containsKey(k))
					{
						int count = map.get(k);
						count = count + count_of_retweeter;
						map.put(k, count);
					}
					else
					{
						map.put(k, count_of_retweeter);
					}
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			if (!map.isEmpty()) {
				// for each entry, emit its key and value
				for (Entry<Integer, Integer> e : map.entrySet()) {
					int sKey = e.getKey();
			    	  int intValue = e.getValue().intValue();
			    	  context.write(new IntWritable(sKey), new IntWritable(intValue));
				}
			}
		}
	}
		
		public static class RFriendCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
		{
			private IntWritable result = new IntWritable();
			public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
			{
				int sum = 0;
				for(IntWritable n:values)
				{
					sum+= n.get();
				}
				result.set(sum);
				context.write(key, result);
			}
		}
		
		public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException
		{
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 4) {
				System.err.println("Usage: RetweetFriendCount <in> <out> <tweeters> <num_reducers>");
				System.exit(2);
			}


			
	        Job job = new Job(conf, "word count");
			job.setJarByClass(RetweetFriendCount.class);
			job.setNumReduceTasks(Integer.parseInt(args[3]));
			job.setMapperClass(RFriendCountMapper.class);
			job.setReducerClass(RFriendCountReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(IntWritable.class);
		
			job.setOutputValueClass(IntWritable.class);
			FileSystem hdfs = FileSystem.get( new URI(args[2]), conf );
			Path dir = new Path(otherArgs[2]);
			PathFilter filter = new PathFilter() {
				public boolean accept(Path file){
					return file.getName().contains("part-r-");
				}
			};
			FileStatus[] files = hdfs.listStatus(dir, filter);
	        Arrays.sort(files);
	        for (int i = 0; i != files.length; i++) {
	        	System.out.println("Importing " + files[i].getPath());
	        	DistributedCache.addCacheFile(files[i].getPath().toUri(), conf);
	        }
			
			
			//DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(), job.getConfiguration());
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
