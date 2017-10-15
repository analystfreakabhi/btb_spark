
# coding: utf-8

# In[1]:

import datetime
from pytz import timezone
print "Last run @%s" % (datetime.datetime.now(timezone('US/Pacific')))


# In[2]:

from pyspark.context import SparkContext
print "Running Spark Version %s" % (sc.version)


# In[3]:

from pyspark.conf import SparkConf
conf = SparkConf()
print conf.toDebugString()


# In[4]:

# Read Orders
orders = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('NW/NW-Orders.csv')


# In[5]:

order_details = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('NW/NW-Order-Details.csv')


# In[6]:

products = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('NW/NW-Products.csv')


# In[7]:

orders.count()


# In[8]:

# Save as parquet format for folks who couldn't make spark-csv work
orders.repartition(1).write.mode("overwrite").format("parquet").save("orders.parquet")
order_details.repartition(1).write.mode("overwrite").format("parquet").save("order_details.parquet")
products.repartition(1).write.mode("overwrite").format("parquet").save("products.parquet")


# In[9]:

# Read & Check
df = sqlContext.read.load("orders.parquet")
df.show(5)
df.count()


# In[10]:

# Read & Check
df = sqlContext.read.load("products.parquet")
df.show(5)
df.count()


# In[11]:

# Read & Check
df = sqlContext.read.load("order_details.parquet")
df.show(5)
df.count()


# In[12]:

order_details.count()


# In[13]:

orders.show(10)


# In[14]:

order_details.show(10)


# In[15]:

products.count()


# In[16]:

products.show(1)


# In[17]:

# Questions
# 1. How many orders were placed by each customer? 
# 2. How many orders were placed by each country ?
# 3. How many orders by month/year ?
# 4. Total Sales for each customer by year
# 5. Average order by customer by year
# These are questions based on customer and sales reports
# Similar questions can be asked about products as well


# In[18]:

orders.dtypes


# In[19]:

# 1. How many orders were placed by each customer? 
orders.groupBy("CustomerID").count().orderBy("count",ascending=False).show(10)


# In[20]:

# 2. How many orders were placed by each country ?
orders.groupBy("ShipCuntry").count().orderBy("count",ascending=False).show(10)


# In[21]:

# For the next set of questions, let us transform the data
# 1. Add OrderTotal column to the Orders DataFrame
# 1.1. Add Line total to order details
# 1.2. Aggregate total by order id
# 1.3. Join order details & orders to add the order total
# 1.4. Check if there are any null columns
# 2. Add a date column
# 3. Add month and year


# In[22]:

# 1.1. Add Line total to order details
order_details_1 = order_details.select(order_details['OrderID'],
                                       (order_details['UnitPrice'].cast('float') *
                                       order_details['Qty'].cast('float') *
                                       (1.0 -order_details['Discount'].cast('float'))).alias('OrderPrice'))


# In[23]:

order_details_1.show(10)


# In[24]:

# 1.2. Aggregate total by order id
order_tot = order_details_1.groupBy('OrderID').sum('OrderPrice').alias('OrderTotal')


# In[25]:

order_tot.orderBy('OrderID').show(5)


# In[26]:

# 1.3. Join order details & orders to add the order total
orders_1 = orders.join(order_tot, orders['OrderID'] == order_tot['OrderID'], 'inner').select(orders['OrderID'],
        orders['CustomerID'],
        orders['OrderDate'],
        orders['ShipCuntry'].alias('ShipCountry'),
        order_tot['sum(OrderPrice)'].alias('Total'))


# In[27]:

orders_1.orderBy('CustomerID').show()


# In[28]:

# 1.4. Check if there are any null columns
orders_1.filter(orders_1['Total'].isNull()).show(40)


# In[29]:

import pyspark.sql.functions as F
from pyspark.sql.types import DateType,IntegerType
from datetime import datetime
convertToDate = F.udf(lambda s: datetime.strptime(s, '%m/%d/%y'), DateType())
#getMonth = F.udf(lambda d:d.month, IntegerType())
#getYear = F.udf(lambda d:d.year, IntegerType())
getM = F.udf(lambda d:d.month, IntegerType()) # To test UDF in 1.5.1. didn't work in 1.5.0
getY = F.udf(lambda d:d.year, IntegerType())


# In[30]:

# 2. Add a date column
orders_2 = orders_1.withColumn('Date',convertToDate(orders_1['OrderDate']))


# In[31]:

orders_2.show(2)


# In[32]:

# 3. Add month and year
#orders_3 = orders_2.withColumn('Month',getMonth(orders_2['Date'])).withColumn('Year',getYear(orders_2['Date']))
orders_3 = orders_2.withColumn('Month',F.month(orders_2['Date'])).withColumn('Year',F.year(orders_2['Date']))
orders_3 = orders_2.withColumn('Month',getM(orders_2['Date'])).withColumn('Year',getY(orders_2['Date']))


# In[33]:

orders_3.show(5)


# In[34]:

# 3. How many orders by month/year ?
import time
start_time = time.time()
orders_3.groupBy("Year","Month").sum('Total').show()
print "%s Elapsed : %f" % (datetime.today(), time.time() - start_time)
#[7/3/15 8:20 PM 1.4.1] Elapsed : 22.788190 (with UDF)
#[1.5.0] 2015-09-05 10:29:57.377955 Elapsed : 10.542052 (with F.*)
#[1.5.1] 2015-09-24 17:53:13.605858 Elapsed : 11.024428 (with F.*)
#[2.0.0] 2016-07-15 13:24:19.965315 Elapsed : 2.917482


# In[35]:

# 4. Total Sales for each customer by year
import time
start_time = time.time()
orders_3.groupBy("CustomerID","Year").sum('Total').show()
print "%s Elapsed : %f" % (datetime.today(), time.time() - start_time)
#[1.4.1] 2015-07-03 20:29:37.499064 Elapsed : 18.372916 (with UDF)
#[1.5.0] 2015-09-05 10:26:14.689536 Elapsed : 11.468665 (with F.*)
#[1.5.1] 2015-09-24 17:53:23.670811 Elapsed : 10.057430 (with F.*)
#[2.0.0] 2016-07-15 13:24:21.864272 Elapsed : 1.888228


# In[36]:

# 5. Average order by customer by year
import time
start_time = time.time()
orders_3.groupBy("CustomerID","Year").avg('Total').show()
print "%s Elapsed : %.2f" % (datetime.today(), time.time() - start_time)
#[1.4.1] 2015-07-03 20:32:14.734800 Elapsed : 18.88 (with UDF)
#[1.5.0] 2015-09-05 10:26:28.227042 Elapsed : 13.53 (with F.*)
#[1.5.1] 2015-09-24 17:55:25.963050 Elapsed : 10.02 (with F.*)
#[2.0.0] 2016-07-15 13:24:23.394839 Elapsed : 1.52


# In[37]:

# 6. Average order by customer
import time
start_time = time.time()
orders_3.groupBy("CustomerID").avg('Total').orderBy('avg(Total)',ascending=False).show()
print "%s Elapsed : %.2f" % (datetime.today(), time.time() - start_time)
#[1.4.1] 2015-07-03 20:33:21.634902 Elapsed : 20.15 (with UDF)
#[1.5.0] 2015-09-05 10:26:40.064432 Elapsed : 11.83 (with F.*)
#[1.5.1] 2015-09-24 17:55:49.818042 Elapsed : 9.43 (with F.*)
#[2.0.0] 2016-07-15 13:24:25.973737 Elapsed : 2.57


# In[ ]:



