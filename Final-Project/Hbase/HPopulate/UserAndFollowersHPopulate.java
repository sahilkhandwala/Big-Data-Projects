package Sahil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 

public class UserAndFollowersHPopulate {
	
		public static class TweeterMapper<K,V> extends Mapper <LongWritable, Text, K, V> 
		{
			private HTable table;
			int l = 0;
			int k=0;
			
			public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				if(value.toString().length()==0)
				{
					k++;
				}
				
				else 
				{
				String parts[] = value.toString().split("\t");
	        	String user = parts[0];
	        	String followers;
	        	if(parts.length<3)
	        	followers = "0";
	        	else
	        	followers = parts[3];	

				if(user.length()==0 || followers.length()==0)
				l++;
				
				else
				{
				byte[] rowKey = Bytes.toBytes(user);
				Put p = new Put(rowKey);
				p.setWriteToWAL(false);
				p.add(Bytes.toBytes("Followers"), Bytes.toBytes("Followers"), Bytes.toBytes(followers));
				table.put(p);
				//context.write(new ImmutableBytesWritable(rowKey), p);
				}
			}
		}		
			
			protected void setup(Context context) throws IOException {
	    	  this.table = new HTable(context.getConfiguration(), "UserAndFollowers");
	    	  this.table.setAutoFlush(false);
			}
			
			protected void cleanup(Context context) throws IOException
			{
				table.flushCommits();
				table.close();
			}
		}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
			
			if(args.length != 1) {
				System.err.println("Error");
				System.exit(2);
			}
			
			byte[][] splitter = {Bytes.toBytes("1"),Bytes.toBytes("2"),Bytes.toBytes("3"),Bytes.toBytes("4"),
					Bytes.toBytes("5"), Bytes.toBytes("6"), Bytes.toBytes("7"), Bytes.toBytes("8"),
					Bytes.toBytes("9")};
			
			Configuration conf1 = HBaseConfiguration.create(new Configuration());
			
			HTableDescriptor ht = new HTableDescriptor("UserAndFollowers");
			ht.addFamily( new HColumnDescriptor("Followers") );
			HBaseAdmin hba = new HBaseAdmin(conf1);
			
			if(hba.tableExists("UserAndFollowers"))
			{
				hba.disableTable("UserAndFollowers");
				hba.deleteTable("UserAndFollowers");
			}
			hba.createTable(ht, splitter);
			Job job = new Job(conf1);
			job.setNumReduceTasks(0);
			job.setJarByClass(UserAndFollowersHPopulate.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
		    job.setMapperClass(TweeterMapper.class);
		    //We're not reducing, simply using the mapper to write
		    job.setOutputFormatClass(TableOutputFormat.class);
		    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "UserAndFollowers");
		    hba.close();
		    try {
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} 
		}
	}