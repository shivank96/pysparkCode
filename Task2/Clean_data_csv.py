#"""------------------------------Cleaning null values of a csv file---------------------------------------"""
#------------------------------------------------------------------------------------------------------------
#Program to clean null values and fill them with average of previous two values
#Module : 
#Author :
#Date :
#------------------------------------------------------------------------------------------------------------
import sys
from pyspark.sql import SparkSession , SQLContext
from pyspark import SparkContext as sc, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#================Function for cleaning null values=================
def clean_Null():
    #creating spark session
    spk_session=SparkSession.builder.appName('Fit_PSSQLApp01').getOrCreate()
  
    #reading csv file and creating a data frame
    df = spk_session.read.format('csv').option('header',True).load('file_path.csv')
 
    df.show()

    #Adding a column to have a hold on current row
    df = df.withColumn("Serial no",F.monotonically_increasing_id()+1)

    #using filter() and isNull() are helpful in finding out if there exists any null values
    #checking each columns existence for null values 
    if (df.filter(df["Earnings"].isNull())):
        #calling clean_Null_Values_Of_Earnings()
        df = clean_Null_of_Earnings(df)
    if (df.filter(df["SP500"].isNull())):
        #calling clean_Null_Values_Of_Earnings()
        df = clean_Null_Values_Of_SP500(df)
  
    if (df.filter(df["Long Interest Rate"].isNull())):
        #calling clean_Null_Values_Of_Earnings()
        df = clean_Null_Values_Of_Long_Interest_Rate(df)

    if (df.filter(df["Consumer Price Index"].isNull())):
        #calling clean_Null_Values_Of_Consumer_Price_Index()
        df = clean_Null_Values_Of_Consumer_Price_Index(df)

    if (df.filter(df["Real Earnings"].isNull())):
        #calling clean_Null_values_Of_RealEarnings()
        df = clean_Null_values_Of_RealEarnings(df)

    if (df.filter(df["Dividend"].isNull())):
        #calling clean_Null_values_Of_RealEarnings()
        df = clean_Null_values_Of_Dividend(df)
  
    if (df.filter(df["Real Price"].isNull())):
        #calling clean_Null_values_Of_Real_Price()
        df = clean_Null_values_Of_Real_Price(df)

    if (df.filter(df["Real Dividend"].isNull())):
        #calling clean_Null_values_Of_RealEarnings()
        df = clean_Null_values_Of_RealDividend(df)
  
    if (df.filter(df["PE10"].isNull())):
        df_avg = df.agg(F.avg(F.col("PE10")))
        df_avg.show()
        df = df.na.fill("16.851",["PE10"])
        df=df.drop("Serial no")

    df.show()

    #creating a new csv fill without null values(cleaned data) using data_frame (df)
    create_cleaned_csv(df)
  
#================Function for cleaning null values of Earnings column=================
def clean_Null_of_Earnings(df):
    df = df.withColumn('cumsum', F.sum('Earnings').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('Earnings').isNull())
    df = df.withColumn('Earnings', F.when(condition_col, F.col('E1')).otherwise(F.col('Earnings')))
    df=df.drop("cumsum","E1")
    return df
  
#================Function for cleaning null values of SP500 column=================
def clean_Null_Values_Of_SP500(df):
    df = df.withColumn('cumsum', F.sum('SP500').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('SP500').isNull())
    df = df.withColumn('SP500', F.when(condition_col, F.col('E1')).otherwise(F.col('SP500')))
    df=df.drop("cumsum","E1")
    return df

#================Function for cleaning null values of Consumer Price Index column=================
def clean_Null_Values_Of_Consumer_Price_Index(df):
    df = df.withColumn('cumsum', F.sum('Consumer Price Index').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('Consumer Price Index').isNull())
    df = df.withColumn('Consumer Price Index', F.when(condition_col, F.col('E1')).otherwise(F.col('Consumer Price Index')))
    df=df.drop("cumsum","E1")
    return df

#================Function for cleaning null values of Long Interest Rate column=================
def clean_Null_Values_Of_Long_Interest_Rate(df):
    df = df.withColumn('cumsum', F.sum('Long Interest Rate').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('Long Interest Rate').isNull())
    df = df.withColumn('Long Interest Rate', F.when(condition_col, F.col('E1')).otherwise(F.col('Long Interest Rate')))
    df=df.drop("cumsum","E1")
    return df

#================Function for cleaning null values of Real Price column=================
def clean_Null_values_Of_Real_Price(df):
    df = df.withColumn('cumsum', F.sum('Real Price').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('Real Price').isNull())
    df = df.withColumn('Real Price', F.when(condition_col, F.col('E1')).otherwise(F.col('Real Price')))
    df=df.drop("cumsum","E1")
    return df

#================Function for cleaning null values of Real Earnings=================
def clean_Null_values_Of_RealEarnings(df):
    df = df.withColumn('cumsum', F.sum('Real Earnings').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('Real Earnings').isNull())
    df = df.withColumn('Real Earnings', F.when(condition_col, F.col('E1')).otherwise(F.col('Real Earnings')))
    df=df.drop("cumsum","E1")
    return df

#================Function for cleaning null values of Dividend======================
def clean_Null_values_Of_Dividend(df):
    df = df.withColumn('cumsum', F.sum('Dividend').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('Dividend').isNull())
    df = df.withColumn('Dividend', F.when(condition_col, F.col('E1')).otherwise(F.col('Dividend')))
    df=df.drop("cumsum","E1")
    return df

#================Function for cleaning null values of Real Dividend=================
def clean_Null_values_Of_RealDividend(df):
    df = df.withColumn('cumsum', F.sum('Real Dividend').over(Window.partitionBy().orderBy('Serial no').rowsBetween(-sys.maxsize, 0)))
    df=df.withColumn("E1",F.col("cumsum")/F.col("Serial no"))
    condition_col = (F.col('Real Dividend').isNull())
    df = df.withColumn('Real Dividend',F.when(condition_col, F.col('E1')).otherwise(F.col('Real Dividend')))
    df=df.drop("cumsum","E1")
    return df

#===============Function for creating a new csv file=================================
def create_cleaned_csv(df):
    df.write.csv('new_cleaned.csv', header= True)

#calling clean_Null() function 
clean_Null()
