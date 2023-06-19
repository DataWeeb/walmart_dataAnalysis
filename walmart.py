from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,max,format_number,col,desc,min


my_conf=SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=my_conf).getOrCreate()

df1=spark.read.csv("D:\Ravi\DE\spark-walmart-data-analysis-exercise-master\spark-walmart-data-analysis-exercise-master\walmart_stock.csv",\
                   header=True,inferSchema=True)

#Q1 What are the column names?

print(df1.columns)

#Q2 What does the Schema look like?

df1.printSchema()

#Q3 Print out the first 5 columns ?

for i in df1.head(5):
    print(i,',')

df1.show(5)

df1.describe().show()

#Q4 round all columns upto 2 decimal places


summary = df1.describe()
summary.select(summary['summary'],
 format_number(summary['Open'].cast('float'), 2).alias('Open'),
 format_number(summary['High'].cast('float'),2).alias('High'),
 format_number(summary['Low'].cast('float'),2).alias('Low'),
 format_number(summary['Close'].cast('float'),2).alias('Close'),
 format_number(summary['Volume'].cast('float'),2).alias('Volume'),
 format_number(summary['Adj Close'].cast('float'),2).alias('Adj Close'))

df1.show()


# Q5 Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus
# volume of stock traded for a day.

df2=df1.withColumn("HV ratio",col("High")/col("Volume")).select(["HV ratio"])
df2.show()

# Q6 What day had the Peak High in Price ?

df1.orderBy(df1["High"].desc()).select(['Date']).limit(1).show()

# Q7 What is the mean of close column ?

df1.select(mean('Close')).show()

#Q8 What is the max and minimum of volume column ?

df1.select(max('Volume').alias('max'),min('Volume').alias('min')).show()

#Q9 How many days was close lower than 60 dollars ?

df1.createOrReplaceTempView('walmart')

spark.sql("select Date from walmart where Close>60").show()
        #OR
df1.select(col("Date")).where(col("Close")>60).show()
        #OR
print(df1.filter(col("Close")<60).count())

#Q10 What percentage of the time was the High greater than 80 dollars ?
# In other words, (Number of Days High>80)/(Total Days in the dataset)


spark.sql("select (select count(Date) from walmart where High>80)/count(Date) * 100 as ratio from walmart ").show()

#OR

print(df1.filter(col("High")>80).count()*100/df1.count())

#Q11 What is the max High per year?

from pyspark.sql.functions import year

df2=df1.withColumn("year",year("Date"))

df2.createOrReplaceTempView("walmart2")

spark.sql("select year,max(High) from walmart2 group by year").show()

#or

df2.groupBy("year").agg(max("High").alias("maximum")).show()

#Q12 What is the average Close for each Calendar Month?
#In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your
#result will have a value for each of these months.

from pyspark.sql.functions import month
df2=df1.withColumn("month",month("Date"))

df2.createOrReplaceTempView("walmart3")

spark.sql("select month,avg(Close) from walmart3 group by month order by month").show()

#or
from pyspark.sql.functions import mean

df2.groupBy("month").agg(mean("Close").alias("average")).orderBy(col("month")).show()

