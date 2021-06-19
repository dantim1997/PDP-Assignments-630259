--Import CSVExcelStorage into the script to use CSV inside the code.
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

--Get the orders.csv file from the HDFS location.
All_orders = LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVExcelStorage() AS
(game_id:int,unit_id:int,unit_order:chararray,location:chararray,target:chararray,target_dest:chararray,success:int,reason:int,turn_num:int);

--This will filter only the rows that have Holland as a target.
Orders_target_holland = FILTER All_orders BY target == 'Holland';

--Group every row by location.
Orders_grouped_by_location = GROUP Orders_target_holland BY (location,target);

--This will count the amount Holland was a target based on the grouped location.
counted_locations = FOREACH Orders_grouped_by_location GENERATE group, COUNT(Orders_target_holland);

--This will order the locations ascending based on the alphabet.
Counted_locations_ascending = ORDER counted_locations BY $0 ASC;

--Print all the rows with a DUMP
--It first shows the location then the target and as last the count of how many.
DUMP Counted_locations_ascending;