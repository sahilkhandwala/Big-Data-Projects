package CS6240;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.el.OrOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class Flight {
	
	// create global counters
	public static enum Global_Counters {
		count,
		averageFlightDelay
	};
	
	public static class FlightDataMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			// using CSV parser to pass the input file
			CSVParser parser = new CSVParser();
			
		    String origin = null;
		    String destination = null;
		    String diverted = null;
		    String cancelled = null;
		    String year = null;
		    String month = null;
		    String flightdate = null;
		    String DepartureTime = null;
		    String ArrivalTime = null;
		    String ArrivalDelay = null;
			
			
			String[] line = parser.parseLine(value.toString());
			origin = line[11];
			destination = line[17];
			diverted = line[43];
			cancelled = line[41];
			year = line[0];
			month = line[2];
			flightdate = line[5];
			DepartureTime = line[24];
			ArrivalTime = line[35];
			ArrivalDelay = line[37];
			
			// checking if origin is ORD and destination != JFK and checking if the 
			// flight is not diverted or cancelled and falls within flightdate of
			// >= June 2007 and <= May 2008 
			if(origin.equalsIgnoreCase("ORD") && !destination.equalsIgnoreCase("JFK"))
			{
				if(cancelled.equals("0.00") && diverted.equals("0.00"))
				{
					int yr = Integer.parseInt(year);
					int mon = Integer.parseInt(month);
					
					if((yr == 2007 && mon >= 6) || (yr == 2008 && mon <= 5))
					{
						 // key is set a destination, flightdate
						 Text key1 = new Text(destination + "," + flightdate);
						 Text val = new Text(origin + "," + ArrivalTime + "," + DepartureTime + "," + ArrivalDelay);
						 context.write(key1, val);
					}
				}				
			}
			
			// checking if origin!= ORD and destination = JFK and checking if the 
			// flight is not diverted or cancelled and falls within flightdate of
			// >= June 2007 and <= May 2008 
			else if(!origin.equalsIgnoreCase("ORD") && destination.equalsIgnoreCase("JFK"))
			{
					if(cancelled.equals("0.00") && diverted.equals("0.00"))
					{
						int yr = Integer.parseInt(year);
						int mon = Integer.parseInt(month);
						
						if((yr == 2007 && mon >= 6) || (yr == 2008 && mon <= 5))
						{
							//key is set as origin, flightdate
							Text key1 = new Text(origin + "," + flightdate);
							Text val = new Text(origin + "," + ArrivalTime + "," + DepartureTime + "," + ArrivalDelay);
							context.write(key1, val);
						}
					}
			}
		}
	}
	
	public static class FlightDataReducer extends Reducer<Text, Text, Text, Text> 
	{	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException
		{
			// 2 lists are made to store the data
			ArrayList<String[]> list1 = new ArrayList<String[]>();
			ArrayList<String[]> list2 = new ArrayList<String[]>();
			
			for(Text v : values)
			{
				String[] record = new CSVParser().parseLine(v.toString());
				if(record[0].equalsIgnoreCase("ORD"))
				{	// if origin is ORD we are adding to one list
					list1.add(record);
				}
				else
				{
					// if not we are adding to another list
					list2.add(record);
				}	
			}
			
			//String[] flight1;
			//String[] flight2;
			
			for(String[] flight1: list1)
			{
				for(String[] flight2: list2)
				{
					// checking if arrival time of 1 flight which matches all the conditions is < the departure time 
					// of another flight which also matches the conditions
						if(Double.parseDouble(flight1[1]) < Double.parseDouble(flight2[2]))
						{
							// updating global counters
							// incrementing totalflightcount by 1 and total delay by the arrivaldelay of flight1 + arrivaldelay of flight2
							context.getCounter(Global_Counters.averageFlightDelay).increment((long) (Float.parseFloat(flight1[3])+ Float.parseFloat(flight2[3])));		
							context.getCounter(Global_Counters.count).increment(1);		
						}
					}
				}
			}	
		}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("Error");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Flight Data Join");
		job.setJarByClass(Flight.class);
		job.setMapperClass(FlightDataMapper.class);
		job.setReducerClass(FlightDataReducer.class);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    if (job.waitForCompletion(true)) {
	    	long avgDelay = (long)job.getCounters().findCounter(Global_Counters.averageFlightDelay).getValue();
	    	long totalFlightCount = (long)job.getCounters().findCounter(Global_Counters.count).getValue();
	    	
	    	//getting total avergae flight delay by dividing avgdelay by totalflightcount
	    	
	    	double totalAverageFlightDelay = (float)avgDelay/(float)totalFlightCount; 
	    	System.out.println("Total Delay " + avgDelay);
	    	System.out.println("TotalFlightCount "+ totalFlightCount);
	    	System.out.println("Average Flight Delay for 2-Legged Flight is " + totalAverageFlightDelay);
	    }
	}
}


