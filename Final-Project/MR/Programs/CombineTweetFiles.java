import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CombineTweetFiles {
	
	public static void main(String args[]) throws IOException {
		//List<String> results = new ArrayList<String>();
		int max_size = 114000000 / 2;
		int current_name = 0;
		long current_size = 0;
		String header = "TWEETER: ";

		File[] files = new File(args[0]).listFiles();
		//Arrays.sort(files);
		BufferedWriter output = null;
		BufferedReader reader;
		File new_file = new File(args[1] + Integer.toString(current_name));
		if (new_file.createNewFile()){
	        System.out.println("File is created: " + Integer.toString(current_name));
        	output = new BufferedWriter(new FileWriter(new_file));
        	current_size += new_file.length();
        	//current_size += Integer.toString(current_name).length();
		}
		for (File file : files) {
		    if (file.isFile() && org.apache.commons.lang.StringUtils.isNumeric(file.getName())) {
		    	if (file.length() + header.length() + file.getName().length() + current_size <= max_size) {
		    		output.write(header + file.getName() + "\n");
		    		current_size += (header + Integer.toString(current_name)).length();
		    		
		    		reader = new BufferedReader(new FileReader(file));
					String line = null;
					while ((line = reader.readLine()) != null) {
						//erasing white space
					     if (line.trim().length() != 0) {
					    	 output.write(line + "\n");
					    	 current_size += line.length();
					     }
					}
					reader.close();
		    	}
		    	else {
		    		output.close();
		    		current_size = 0;
		    		current_name += 1;
					
		    		new_file = new File(args[1] + Integer.toString(current_name));
		    		if (new_file.createNewFile()){
		    	        System.out.println("File is created: " + Integer.toString(current_name));
		            	output = new BufferedWriter(new FileWriter(new_file));
		            	current_size += new_file.length();
		            	//current_size += Integer.toString(current_name).length();
		            	
		            	output.write(header + file.getName() + "\n");
			    		current_size += (header + Integer.toString(current_name)).length();
			    		
			    		reader = new BufferedReader(new FileReader(file));
						String line = null;
						while ((line = reader.readLine()) != null) {
							//erasing white space
						     if (line.trim().length() != 0) {
						    	 output.write(line + "\n");
						    	 current_size += line.length();
						     }
						}
						reader.close();
		    		}
		    		else {
		    			System.out.println("File not created!");
		    		}
		    	}
		    	
		    }
		}	    
	}
}
