package Sahil;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class HbaseRetweetFriendCount {
	
	public static class RetweeterCountMapper<K,V> extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] in = value.toString().split("\t");
		String k = in[0];
		String v = in[1];
		context.write(new Text(k), new LongWritable(Long.parseLong(v)));
	}
  }
	
	public static class RetweeterCountReducer<K,V> extends Reducer<Text,LongWritable, K,V> 
	{
		HTable table = null;
		HTable newTable;
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException
		{
			HashMap<Long, Integer> map = new HashMap<Long, Integer>();
			
			for(LongWritable l: values)
			{
				if(map.containsKey(l.get()))
				{
					int count = map.get(l.get());
					count++;
					map.put(l.get(), count);
				}
				else
				{
					map.put(l.get(), 1);
				}
			}
			
			List<Get> queryList = new ArrayList<Get>();
			int finalCount = 0;
			
			for(Entry<Long, Integer> entry : map.entrySet())
		      {
				  String finalKey = String.valueOf(entry.getKey());
				  Get g = new Get(Bytes.toBytes(""+key.toString()+finalKey));
				  Result r = table.get(g);
				  if(r.isEmpty() || r==null);
					else
					{
						byte[] vHbase = r.getValue(Bytes.toBytes("Col"), Bytes.toBytes("Follower"));
						long tempKey = Long.parseLong(Bytes.toString(vHbase));
						if(map.containsKey(tempKey))
						{
							finalCount= finalCount+map.get(tempKey);
						}
					}
		    	  queryList.add(new Get(Bytes.toBytes(key.toString()+","+finalKey)));
		      }
			
			Result[] results = table.get(queryList); 
			for(Result r: results)
			{
				if(r.isEmpty() || r==null);
				else
				{
					byte[] vHbase = r.getValue(Bytes.toBytes("Col"), Bytes.toBytes("Follower"));
					long tempKey = Long.parseLong(Bytes.toString(vHbase));
					if(map.containsKey(tempKey))
					{
						finalCount= finalCount+map.get(tempKey);
					}
				}
			}
			
			if(finalCount>0)
			{
				byte[] rowKey = Bytes.toBytes(key.toString());
				Put p = new Put(rowKey);
				p.setWriteToWAL(false);
				String vv =String.valueOf(finalCount);
				p.add(Bytes.toBytes("Col1"),Bytes.toBytes("CountOfFriendRetweeters"),Bytes.toBytes(vv));
				newTable.put(p);
			}
		}
		
		@Override
		public void setup(Context context) throws IOException
		{
			this.newTable = new HTable(context.getConfiguration(), "ResultTable2");
			this.newTable.setAutoFlush(false);
			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, "NetworkData");
		}
		
		@Override
		public void cleanup(Context context) throws IOException
		{
			newTable.flushCommits();
			newTable.close();
			table.close();
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "RetweetFriends");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
	      System.err.println("Usage: HbaseRetweetFriendCount <in> <Reduce Tasks>");
	      System.exit(2);
         }
        
        Configuration conf1 = HBaseConfiguration.create(new Configuration());
        HTableDescriptor ht = new HTableDescriptor("ResultTable2");
		ht.addFamily( new HColumnDescriptor("Col1") );
		HBaseAdmin hba = new HBaseAdmin(conf1);
		
		if(hba.tableExists("ResultTable2"))
		{
			hba.disableTable("ResultTable2");
			hba.deleteTable("ResultTable2");
		}
		hba.createTable(ht);	
		job.setNumReduceTasks(Integer.parseInt(otherArgs[1]));
		
        job.setJarByClass(HbaseRetweetFriendCount.class);
        job.setMapperClass(RetweeterCountMapper.class);
        job.setReducerClass(RetweeterCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "ResultTable2");
	    hba.close();
        //job.setMapOutputKeyClass(LongWritable.class);
        //job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}