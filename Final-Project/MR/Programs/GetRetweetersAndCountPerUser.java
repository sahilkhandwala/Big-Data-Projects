/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//do inmapper combiner
public class GetRetweetersAndCountPerUser {

	public static class TweetMapper 
	extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

		
		String currentLine = "";

		boolean new_tweet = true;

		String origin = "";
		boolean retweet = false;
		int original_tweeter = 0;

		int id_index = 0;
		
		boolean continuing_string = false;
		String current_tweeter = "";
		
		// Finds all tags based on the @ symbol and returns an ArrayList of it
		public ArrayList<Integer> findAllTags(String s) {
			ArrayList<Integer> indexes = new ArrayList<Integer>();
			int index = s.indexOf("@");
			while (index != -1) {
				indexes.add(index);
				index = s.indexOf("@", index + 1);
			}
			return indexes;
		}

		// Finds the last retweet via the last RT @
		public int findLastRetweet(String s) {
			return s.lastIndexOf("RT @") + 3;
		}

		//  Retrives the identity within the MentionedEntities
		public int getIdentityIndex(int index, ArrayList<Integer> indexes) {
			return indexes.indexOf(index);
		}
		
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
				currentLine = value.toString();
				currentLine = currentLine.replaceAll("\\s+", " ");
				if (currentLine.startsWith("TWEETER: ")) {
					current_tweeter = currentLine.replaceFirst("TWEETER: ", "").trim();
				}
				// we have reached the end of a tweet description
				else if (currentLine.equals("***") && new_tweet == false) {
					new_tweet = true;
					if (original_tweeter != Integer.parseInt(current_tweeter) && (retweet && original_tweeter != 0)) {
						context.write(new IntWritable(original_tweeter), 
									new IntWritable(Integer.parseInt(current_tweeter)));
					}
					retweet = false;
					original_tweeter = 0;
					origin = "";
					id_index = 0;
				}
				// new tweeter
				else if (currentLine.equals("***") && new_tweet == true) {
					new_tweet = false;
				}
				// checks to see if the author of this tweet is not this user's
				else if (currentLine.startsWith("Origin: ")) {
					origin = currentLine;
					continuing_string = true;
				}
				else if (continuing_string && !currentLine.startsWith("Text: ")) {
					origin = origin + " " + currentLine;
				}
				else if (currentLine.startsWith("Text: ")) {
					origin.replaceAll("\\s+", " ");
					if (origin.contains("RT @")) {
						id_index = getIdentityIndex(findLastRetweet(origin), findAllTags(origin));
						retweet = true;
					}
					continuing_string = false;
				}
				// must have had author of the original tweet
				// do not want to double count retweets if this tweet is THIS user's
				else if (currentLine.startsWith("RetCount: ") && !currentLine.trim().equals("RetCount: 0"))
					retweet = retweet && true;	
				else if (currentLine.startsWith("MentionedEntities: ") && retweet) {
					String[] current_split = currentLine.split(" ");
					try {
						original_tweeter = Integer.parseInt(current_split[id_index + 1]);
					}catch(ArrayIndexOutOfBoundsException e) {
						// We are unable to get an ID because it is missing
						retweet = false;
					}
				}
				else
					;;//continue;
			
		}
	}

	public static class IntSumReducer 
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				context.write(key, it.next());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: GetRetweetersAndCountPerUser <in> <out> <num_reducers>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(RetweetersPerUser.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		System.out.println(otherArgs[0]);
		job.setMapperClass(TweetMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		if(job.waitForCompletion(true)) {
			FileSystem hdfs = FileSystem.get( new URI(args[1]), conf );
			Path dir = new Path(args[1]);
			PathFilter filter = new PathFilter() {
				public boolean accept(Path file){
					return file.getName().startsWith("part-r-");
				}
	        };
	        
            HashMap<Integer, Integer> counts_for_user = new HashMap<Integer, Integer>();
	        FileStatus[] files = hdfs.listStatus(dir, filter);
	        Arrays.sort(files);
	        for (int i = 0; i != files.length; i++) {
	        	Path pt= files[i].getPath() ;
	            BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
	            String line = null;
	            while ((line = br.readLine()) != null) {
	            	String[] columns = new String[2];
	            	columns = line.split("\t");
    			    int key = Integer.parseInt(columns[0]);
    			    if (counts_for_user.containsKey(key))
    			    	counts_for_user.put(key, counts_for_user.get(key) + 1);
    			    else
    			    	counts_for_user.put(key, 1);
	            }
	            br.close();
	        }
	        
	        FSDataOutputStream fsDataOutputStream = hdfs.create(new Path(otherArgs[1]+"_count"));      
	        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
	        for (Entry<Integer, Integer> e : counts_for_user.entrySet()) {
	            writer.write(e.getKey() + "\t" + e.getValue() + "\n");
			}
	        writer.close();
	        fsDataOutputStream.close();  
	        hdfs.close();
	        System.exit(0);
		}
		System.exit(1);
	}
}
