#import findspark
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("Linear Regression Model").config("spark.executor.memory", "1gb").getOrCreate()
    
sc = spark.sparkContext

# Load data from spark into dataframe
df = spark.read.format("CSV").option("header", "true").load(".\\TitanicCSV\\titanic.csv")

df = df.withColumn("Survived", df["Survived"].cast(IntegerType())) \
    .withColumn("Class", df["Pclass"].cast(IntegerType())) \
    .withColumn("Name", df["Name"].cast(StringType())) \
    .withColumn("Sex", df["Sex"].cast(StringType())) \
    .withColumn("Age", df["Age"].cast(IntegerType())) \
    .withColumn("Siblings/Spouses Aboard", df["Siblings/Spouses Aboard"].cast(IntegerType())) \
    .withColumn("Parents/Children Aboard", df["Parents/Children Aboard"].cast(IntegerType())) \
    .withColumn("PayedFare", df["Fare"].cast(FloatType())) \

#Assignment A
#Extract the survival wich sex and class
df_survived = df.select("Survived", "Sex", "Class")
#Group by class and Sex to know how many on average people survived based on sex and class 
df_survived = df_survived.groupBy("Sex", "Class").avg("Survived")
#Shows the results
df_survived.show()


#Assignment B
#Extract the Ag, class and Survived
df_Child = df.select("Age", "Class", "Survived").toPandas()
#Only get the records of a Child till 10 and is from the third class
df_Child = df_Child[(df_Child['Age'] <= 10) & (df_Child['Class'] == 3)][['Age', 'Survived', 'Class']]
#Get het count from records of the childeren that survived
Childeren_Survived = df_Child[df_Child['Survived'] == 1].count()[0]
#Calculate the probability of that a child survived if it was on the third class
#By dividing the childeren that survived from the total amount onboard and multiply that by 100 to get the percentage of the survived
Survive_Probability = (Childeren_Survived/df_Child.count()[0]*100).item()
#Shows the results
print("probability that a child in the third class will survive: " + str(Survive_Probability))


#Assignment C
#Select the class and PayedFare 
df_payed = df.select("Class", "PayedFare")
#Group by class to know how many on average the people payed 
df_payed = df_payed.groupBy("Class").avg("PayedFare")
#Shows the results
df_payed.show()