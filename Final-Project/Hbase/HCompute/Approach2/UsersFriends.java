package Sahil;

import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class UsersFriends {
	
	public static class FriendCountMapper<K,V> extends Mapper<LongWritable, Text,K,V>
	{
		private HTable newTable;
		HTable table;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			String[] input = value.toString().split("\t");
			String k = input[0];
			String v = input[1];
			Get g = new Get(Bytes.toBytes(k));
			Result r = table.get(g);
			if(r==null || r.isEmpty());
			else
			{
				byte[] vHbase = r.getValue(Bytes.toBytes("Followers"), Bytes.toBytes("Followers"));
				String valueStr = Bytes.toString(vHbase);
				//context.write(new LongWritable(Long.parseLong(k)),new IntWritable(Integer.parseInt(valueStr)));
				byte[] rowKey = Bytes.toBytes(k);
				Put p = new Put(rowKey);
				p.setWriteToWAL(false);
				p.add(Bytes.toBytes("Col1"),Bytes.toBytes("TotalCountOfRetweets"),Bytes.toBytes(v));
				p.add(Bytes.toBytes("Col1"), Bytes.toBytes("TotalCountOfFollowers"), Bytes.toBytes(valueStr));
				newTable.put(p);
			}
		}
		
		@Override
		public void setup(Context context) throws IOException
		{
			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, "UserAndFollowers");
			this.newTable = new HTable(context.getConfiguration(), "ResultTable");
			this.newTable.setAutoFlush(false);
		}
		
		@Override
		public void cleanup(Context context) throws IOException
		{
			newTable.flushCommits();
			newTable.close();
			table.close();
		}
		
		/*public void setup(Context context) throws IOException {
			org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
			  this.table = new HTable(config, "UserAndFollowers");
		}*/
	}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "UsersFriends");
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length != 1) {
		      System.err.println("Usage: HCompute <in>");
		      System.exit(2);
	         }
	        
	        Configuration conf1 = HBaseConfiguration.create(new Configuration());
	        HTableDescriptor ht = new HTableDescriptor("ResultTable");
			ht.addFamily( new HColumnDescriptor("Col1") );
			HBaseAdmin hba = new HBaseAdmin(conf1);
			
			if(hba.tableExists("ResultTable"))
			{
				hba.disableTable("ResultTable");
				hba.deleteTable("ResultTable");
			}
			hba.createTable(ht);	
			job.setNumReduceTasks(0);

	    
	        job.setJarByClass(UsersFriends.class);
	        job.setMapperClass(FriendCountMapper.class);
	        job.setOutputFormatClass(TableOutputFormat.class);
		    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "ResultTable");
		    hba.close();
	        //job.setMapOutputKeyClass(LongWritable.class);
	        //job.setMapOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);	
		}
	}