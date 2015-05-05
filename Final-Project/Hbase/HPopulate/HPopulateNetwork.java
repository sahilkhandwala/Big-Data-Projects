	package Sahil;

import java.io.IOException;
import java.util.Random;

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

public class HPopulateNetwork {
	
	public static class HbaseNetworkMapper<K,V> extends Mapper<LongWritable, Text, K, V>
	{
		private HTable htable;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException
		{
			String[] line = value.toString().split("\t");
			String follower = line[0];
			String originalTweeter = line[1];
			
			Random r = new Random();
			long random = r.nextInt((100-1)+1)+1;
			long timeStamp = System.currentTimeMillis();
			
			byte[] rowKey = Bytes.toBytes(originalTweeter+","+follower);
			
			Put p = new Put(rowKey);
			p.setWriteToWAL(false);
			
			p.add(Bytes.toBytes("Col"), Bytes.toBytes("Follower"), Bytes.toBytes(follower));
			//p.add(Bytes.toBytes("Col"), Bytes.toBytes("OriginalTweeter"), Bytes.toBytes(originalTweeter));
			htable.put(p);
		}
		
		@Override
		public void setup(Context context) throws IOException {
			this.htable = new HTable(context.getConfiguration(), "NetworkData");
			this.htable.setAutoFlush(false);
		}
		
		@Override
		public void cleanup(Context context) throws IOException
		{
			htable.flushCommits();
			htable.close();
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		if(args.length!=2)
		{
			System.err.println("Usage: HBasePopulate <input> <table>");
			System.exit(-1);
		}
		
		byte[][] splitter = {Bytes.toBytes("1"),Bytes.toBytes("2"),Bytes.toBytes("3"),Bytes.toBytes("4"),
				Bytes.toBytes("5"), Bytes.toBytes("6"), Bytes.toBytes("7"), Bytes.toBytes("8"),
				Bytes.toBytes("9")};
		
		Configuration hc = HBaseConfiguration.create(new Configuration());
		HTableDescriptor ht = new HTableDescriptor(args[1]);
		
		ht.addFamily(new HColumnDescriptor("Col"));
		
		HBaseAdmin hba = new HBaseAdmin(hc);
		if(hba.tableExists(args[1]))
		{
			hba.disableTable(args[1]);
			hba.deleteTable(args[1]);
		}
		hba.createTable(ht, splitter);
		Job job = new Job(hc);
		job.setJarByClass(HPopulateNetwork.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    job.setMapperClass(HbaseNetworkMapper.class);
	    //We're not reducing, simply using the mapper to write
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);
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