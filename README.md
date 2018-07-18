# datawarehousing-ass5
## QUERY 1:
```
@trafficschema =
    EXTRACT Tcs_id  int,
            Main    string,
            Midblock_Route  string,
            Side_1_Route    string,
            Side_2_Route    string,
            Activation_Date DateTime,
            Latitude    double,
            Longitude     double,
            Count_Date  DateTime,
            Peak_Hr_Vehicle_Volume   int,
            Peak_Hr_Pedestrian_Volume   int

    FROM "dataass.csv"
    USING Extractors.Csv(encoding: Encoding.UTF8,skipFirstNRows:1);

@subquery1 =
  SELECT
    Main,
    AVG(Peak_Hr_Vehicle_Volume) AS Volume
    FROM @trafficschema
    GROUP BY Main;

OUTPUT @subquery1   
    TO "/output/query1.csv"
    USING Outputters.Csv(outputHeader: true);
```	
	
## QUERY 2:
```
@trafficschema =
    EXTRACT Tcs_id  int,
            Main    string,
            Midblock_Route  string,
            Side_1_Route    string,
            Side_2_Route    string,
            Activation_Date DateTime,
            Latitude    double,
            Longitude     double,
            Count_Date  DateTime,
            Peak_Hr_Vehicle_Volume   int,
            Peak_Hr_Pedestrian_Volume   int

    FROM "dataass.csv"
    USING Extractors.Csv(encoding: Encoding.UTF8,skipFirstNRows:1);

@subquery1 =
  SELECT 
    Main, Peak_Hr_Vehicle_Volume, Peak_Hr_Pedestrian_Volume
    FROM @trafficschema WHERE Count_Date > new DateTime(2013,01,01) ;

@subquery2 =
    SELECT Main, MAX(Peak_Hr_Vehicle_Volume + Peak_Hr_Pedestrian_Volume) AS maxtotal FROM @subquery1 GROUP BY Main ORDER BY maxtotal DESC FETCH 10 ROWS;


OUTPUT @subquery2  
    TO "/output/query2.csv"
    USING Outputters.Csv(outputHeader: true);
```
	
	
## QUERY 3:
```
@trafficschema =
    EXTRACT Tcs_id  int,
            Main    string,
            Midblock_Route  string,
            Side_1_Route    string,
            Side_2_Route    string,
            Activation_Date DateTime,
            Latitude    double,
            Longitude     double,
            Count_Date  DateTime,
            Peak_Hr_Vehicle_Volume   int,
            Peak_Hr_Pedestrian_Volume   int

    FROM "dataass.csv"
    USING Extractors.Csv(encoding: Encoding.UTF8,skipFirstNRows:1);

@subquery1 =
  SELECT 
    Count_Date.Year, SUM(Peak_Hr_Vehicle_Volume + Peak_Hr_Pedestrian_Volume) AS sumtotal
    FROM @trafficschema GROUP BY Count_Date.Year ORDER BY Count_Date.Year ASC FETCH 500 ROWS;



OUTPUT @subquery1  
    TO "/output/query3.csv"
    USING Outputters.Csv(outputHeader: true);
	
	
## QUERY 4:
@trafficschema =
    EXTRACT Tcs_id  int,
            Main    string,
            Midblock_Route  string,
            Side_1_Route    string,
            Side_2_Route    string,
            Activation_Date DateTime,
            Latitude    double,
            Longitude     double,
            Count_Date  DateTime,
            Peak_Hr_Vehicle_Volume   int,
            Peak_Hr_Pedestrian_Volume   int

    FROM "dataass.csv"
    USING Extractors.Csv(encoding: Encoding.UTF8,skipFirstNRows:1);

@subquery1 =
  SELECT 
    (int)Count_Date.DayOfWeek AS weekday, SUM(Peak_Hr_Vehicle_Volume + Peak_Hr_Pedestrian_Volume) AS sumtotal FROM @trafficschema GROUP BY (int)Count_Date.DayOfWeek ORDER BY (int)Count_Date.DayOfWeek ASC FETCH 500 ROWS;



OUTPUT @subquery1  
    TO "/output/query4.csv"
    USING Outputters.Csv(outputHeader: true);
 ```
	
	
## QUERY 5:
```
@trafficschema =
    EXTRACT Tcs_id  int,
            Main    string,
            Midblock_Route  string,
            Side_1_Route    string,
            Side_2_Route    string,
            Activation_Date DateTime,
            Latitude    double,
            Longitude     double,
            Count_Date  DateTime,
            Peak_Hr_Vehicle_Volume   int,
            Peak_Hr_Pedestrian_Volume   int

    FROM "dataass.csv"
    USING Extractors.Csv(encoding: Encoding.UTF8,skipFirstNRows:1);

@subquery1 = SELECT Main, (int)Count_Date.DayOfWeek AS weekday, SUM(Peak_Hr_Vehicle_Volume + Peak_Hr_Pedestrian_Volume) AS sumtotal FROM @trafficschema GROUP BY Main, (int)Count_Date.DayOfWeek ORDER BY Main ASC FETCH 500 ROWS;



OUTPUT @subquery1  
    TO "/output/query5.csv"
    USING Outputters.Csv(outputHeader: true);
```
