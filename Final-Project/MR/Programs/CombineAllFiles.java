import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class CombineAllFiles {

	// Main method
		public static void main (String args[]) throws IOException
		{	
			if (args.length != 3) {
				System.exit(0);
			}
			// Stream to read file
			HashMap<String, String> retweetsByFriends = new HashMap<String, String>();
			String line;
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
			    
		    while ((line = reader.readLine()) != null) {
		    	String split[] = line.split("\t");
		    	retweetsByFriends.put(split[0], split[1]);
		    }
			    		
			reader.close();
			
			reader = new BufferedReader(new FileReader(args[1]));
            FileWriter fileWriter = new FileWriter(args[2]);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            
            bufferedWriter.write("User\tTotal Followers\tTotal RTs\tRTs By Friends\tRTs Not By Friends\n");
            
            while ((line = reader.readLine()) != null) {
            	String split[] = line.split("\t");
            	if (retweetsByFriends.containsKey(split[0])) {
            		int retweetsNotByFriends = Integer.parseInt(split[1]) - Integer.parseInt(retweetsByFriends.get(split[0])); 
            		
            		bufferedWriter.write(split[0] + "\t" + split[2] + "\t" + split[1] + "\t" 
            		+ retweetsByFriends.get(split[0]) + "\t" + retweetsNotByFriends + "\n");
            	}
            }
            
            reader.close();
            bufferedWriter.close();


		}	
}