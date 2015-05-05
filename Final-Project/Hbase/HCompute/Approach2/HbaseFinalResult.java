package Sahil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class HbaseFinalResult {
	
	public static class Mapper1 extends TableMapper<Text,Text> 
	{
		HTable table;
		HashMap<String, String> map;
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException
		{
			String mainGuy = Bytes.toString(row.get());
			String count = new String(value.getValue(Bytes.toBytes("Col1"),Bytes.toBytes("TotalCountOfRetweets")));
			String totalFollowers = new String(value.getValue(Bytes.toBytes("Col1"), Bytes.toBytes("TotalCountOfFollowers")));	
			
			Get g = new Get(Bytes.toBytes(mainGuy));
			Result r = table.get(g);
			if(r==null || r.isEmpty());
			else
			{
				byte[] vHbase = r.getValue(Bytes.toBytes("Col1"), Bytes.toBytes("CountOfFriendRetweeters"));
				String valueStr = Bytes.toString(vHbase);
				int nonFriends = Integer.parseInt(count) - Integer.parseInt(valueStr);
				String result = totalFollowers+"\t"+count+"\t"+valueStr+"\t"+nonFriends;
				map.put(mainGuy, result);
				//context.write(new Text(mainGuy), new Text(result));
			}
		}
		
		@Override
		public void setup(Context context) throws IOException
		{
			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, "ResultTable2");
			map = new HashMap<String, String>();
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			for(Entry<String, String> entry : map.entrySet())
			{
				context.write(new Text(entry.getKey()), new Text(entry.getValue()));
			}
			table.close();
		}
	}
		public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
		{
			Configuration conf = HBaseConfiguration.create();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length != 1) {
		      System.err.println("Usage: HCompute <out>");
		      System.exit(2);
	         }
	        Job job = new Job(conf, "Hbase_FinalCompute");
	        job.setJarByClass(HbaseFinalResult.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        Scan scan = new Scan();
	        scan.setCaching(1000);
	        scan.setCacheBlocks(false);
	        TableMapReduceUtil.initTableMapperJob("ResultTable", scan, Mapper1.class, Text.class,
	                Text.class, job);
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}