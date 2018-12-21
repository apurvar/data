from pyspark import SparkContext, HiveContext
from pyspark.sql.functions import col, udf
from pyspark.sql import SQLContext

sc=SparkContext()

sc.setLogLevel("Error")

hive_context = HiveContext(sc)

# I. Filter out observation types we are not interested in 
df = hive_context.table("weatherraw").where(col("observation_type").isin(['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN', 'EVAP', 'WESD', 'WESF', 'PSUN']))

# II. Pivot the table on th observation_type column, Group By station_id & observation_date
df_pivoted = df.withColumn("observation_value_new", col("observation_value").cast("float")).groupBy("station_id", "observation_date").pivot("observation_type").avg('observation_value_new')

# III. List out observation_types  stored as Tenths of unit ( (Value/10) unit) in raw data. We will want to multiple the observation values by 10 before storing in curated data.
cols_for_unittenth_transformations = ['PRCP', 'TMIN', 'TMAX', 'EVAP', 'WESD', 'WESF']

# IV. Define a function to convert tenthsOfUnits values by multiplying by 10 
convertTenthsOfUnit =  udf (lambda x: '' if x is None else  x/10.0)

# V. Transform the observation_values for observation_types stored as tenths of unit
for column in cols_for_unittenth_transformations:
	df_pivoted = df_pivoted.withColumn(column, convertTenthsOfUnit(col(column)))

# VI. Create curated table using SQL Context
sqlContext = SQLContext(sc)

df_pivoted.createOrReplaceTempView("weather_pivoted_temp_table") 

sqlContext.sql("DROP TABLE IF EXISTS weatherCurated")

sqlContext.sql("""CREATE TABLE weatherCurated AS  SELECT  station_id AS StationIdentifier,  CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(observation_date,'yyyymmdd')) AS DATE) AS  ObservationDate,  CAST(PRCP  AS FLOAT) AS Precipitation, 
CAST(TMAX AS  FLOAT) AS MaxTemparature, CAST(TMIN AS FLOAT) AS MinTemparature, CAST(SNOW AS FLOAT) AS Snowfall, CAST(SNWD AS FLOAT) AS SnowDepth, CAST(EVAP AS FLOAT) AS Evaporation, CAST(WESD AS FLOAT) AS WaterEquivalentSnowDepth, CAST(WESF AS FLOAT) AS WaterEquivalentSnowFall, CAST(PSUN  AS FLOAT) AS Sunshine
FROM weather_pivoted_temp_table""")

