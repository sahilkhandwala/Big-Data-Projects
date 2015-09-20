REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- setting default number of reducers to 10
-- also adding parallel 10 to each line to have 10 reducers in each operation

SET default_parallel 10;

-- loading the data into 2 relations

Flight1 = LOAD 's3://homework34/data_input/data.csv' using CSVLoader() parallel 10;
Flight2 = LOAD 's3://homework34/data_input/data.csv' USING CSVLoader() parallel 10;

-- We are filtering the loaded data where origin='ORD', destination!='JFK' and diverted and cancelled = 0.00 and checking for year and month conditions between June 2007 and May 2008

FilteredFlight1 = FILTER Flight1 BY $11 == 'ORD' and $17 != 'JFK' and (float)$41 == 0.00 and (float)$43 == 0.00 and ((((int)$0==2007) and ((int)$2>=6)) or (((int)$0==2008) and ((int)$2<=5))) parallel 10;

-- We are filtering the loaded data where origin!='ORD', destination='JFK' and diverted and cancelled = 0.00 and checking for year and month conditions between June 2007 and May 2008

FilteredFlight2 = FILTER Flight2 BY $11 != 'ORD' and $17 == 'JFK' and (float)$41 == 0.00 and (float)$43 == 0.00 and ((((int)$0==2007) and ((int)$2>=6)) or (((int)$0==2008) and ((int)$2<=5))) parallel 10;

-- We are only selecting the usefult tuples 

ExtractFlight1 = FOREACH FilteredFlight1 GENERATE (int)$0 as year, (int)$2 as mon, $5 as fldate1, $17 as dest1, (int)$35 as arr, (float)$37 as arrdel1, $11 as origin1 parallel 10;

-- We are only selecting the usefult tuples 

ExtractFlight2 = FOREACH FilteredFlight2 GENERATE (int)$0 as year, (int)$2 as mon, $5 as fldate2, $11 as origin2, (int)$24 as dep, (float)$37 as arrdel2, $17 as dest2 parallel 10;

-- Joining the Relation 1 by destination, flightdate and Relation 2 by origin, flightdate

JoinFlight = Join ExtractFlight1 BY (dest1, fldate1), ExtractFlight2 BY (origin2, fldate2) parallel 10;

-- After joining the 2 relations we are going to filter flights such that arrivaltime should be less than departure time

FilterByTime = FILTER JoinFlight BY arr < dep parallel 10;

-- We are checking if the month and year exists between June 2007 and May 2008 both inclusive

aggregation= FOREACH FilterByTime GENERATE (float) arrdel1 + arrdel2 as totdel parallel 10;

aggregation1 = GROUP aggregation ALL parallel 10;

-- We are calculating the average total delay

out = FOREACH aggregation1 GENERATE AVG(aggregation.totdel) parallel 10;

-- Storing the output result into a file

STORE out INTO 's3://homework34/output/PigLatin/Prog3/Result';
