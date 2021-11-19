from pyspark.sql.functions import udf
from pyspark.sql.types import *

schema_aisle = StructType().add("aisle_id",IntegerType(),False).add("aisle",StringType(),True)
df_aisle=spark.read.format("csv").option("header", "true").schema(schema_aisle).load("/home/fai10089/project_data/Data_sets/aisles.csv")
df_aisle.printSchema()
df_aisle.show()
df_aisle.filter(df_aisle.aisle_id.isNull() | df_aisle.aisle.isNull()).show()

schema_prior_order = StructType().add("order_id",IntegerType(),True).add("product_id",IntegerType(),True).add("add_to_cart_order",IntegerType(),True).add("reordered",IntegerType(),True)
df_prior_order=spark.read.format("csv").option("header", "true").schema(schema_prior_order).load("/home/fai10089/project_data/Data_sets/prior_order.csv")
df_prior_order.printSchema()
df_prior_order.show()
df_prior_order.filter(df_prior_order.order_id.isNull() | df_prior_order.product_id.isNull() | df_prior_order.add_to_cart_order.isNull() | df_prior_order.reordered.isNull()).show()


schema_products = StructType().add("product_id",IntegerType(),False).add("product_name",StringType(),True).add("aisle_id",IntegerType(),True).add("department_id",IntegerType(),True)
df_products=spark.read.format("csv").option("header", "true").schema(schema_products).load("/home/fai10089/project_data/Data_sets/products.csv")
df_products.printSchema()
df_products.show()
df_products.filter(df_products.product_id.isNull() | df_products.product_name.isNull() | df_products.aisle_id.isNull() | df_products.department_id.isNull()).show()


schema_orders = StructType().add("order_id",IntegerType(),False).add("user_id",IntegerType(),True).add("eval_set",StringType(),True).add("order_number",IntegerType(),True).add("order_dow",IntegerType(),True).add("order_hour_of_day",IntegerType(),True).add("days_since_prior_order",IntegerType(),True)
df_orders=spark.read.format("csv").option("header", "true").schema(schema_orders).load("/home/fai10089/project_data/Data_sets/orders.csv")
df_orders.printSchema()
df_orders.show()
df_orders.filter(df_orders.order_id.isNull() | df_orders.user_id.isNull() | df_orders.eval_set.isNull() | df_orders.order_number.isNull() | df_orders.order_dow.isNull() | df_orders.order_hour_of_day.isNull() ).show()


schema_departments = StructType().add("department_id",IntegerType(),False).add("department",StringType(),True)
df_departments=spark.read.format("csv").option("header", "true").schema(schema_departments).load("/home/fai10089/project_data/Data_sets/departments.csv")
df_departments.printSchema()
df_departments.show()
df_departments.filter(df_departments.department_id.isNull() | df_departments.department.isNull()).show()


schema_train_order = StructType().add("order_id",IntegerType(),True).add("product_id",IntegerType(),True).add("add_to_cart_order",IntegerType(),True).add("reordered",IntegerType(),True)
df_train_order=spark.read.format("csv").option("header", "true").schema(schema_train_order).load("/home/fai10089/project_data/Data_sets/train_order.csv")
df_train_order.printSchema()
df_train_order.show()
df_train_order.filter(df_train_order.order_id.isNull() | df_train_order.product_id.isNull() | df_train_order.add_to_cart_order.isNull() | df_train_order.reordered.isNull()).show()

df_join=df_orders.join(df_train_order,['order_id'],"left")
df_orders_train=df_join.filter(df_join.product_id.isNotNull())
df_join_prior=df_join.filter(df_join.product_id.isNull()).select("order_id","user_id","eval_set","order_number","order_dow","order_hour_of_day","days_since_prior_order")
df_orders_prior=df_join_prior.join(df_prior_order,['order_id'],"left")
df_orders_prior.filter(df_orders_prior.product_id.isNull()).show()

union_orders=df_orders_prior.union(df_orders_train)

union_orders.registerTempTable('table')

latest_cart_q = "SELECT distinct user_id,collect_list(product_id) OVER (PARTITION BY user_id) as latest_cart FROM (SELECT *, MAX(order_number) OVER (PARTITION BY user_id) AS maxON FROM table) M WHERE order_number = maxON"
user_prod_q="Select user_id,product_id ,count(*) as user_product_total_orders ,sum(add_to_cart_order) as user_prod_sum_add_to_cart_order FROM table group By user_id,product_id"
prod_q="Select product_id,count(*) as product_total_orders,sum(add_to_cart_order) as product_sum_add_to_cart_order from table group By product_id"
user_q="Select user_id,max(order_number) as user_total_orders,count(distinct product_id) as user_total_products,sum(add_to_cart_order) as user_sum_add_to_cart_order,count(*) as total_orders from table group By user_id"
incart_q="select distinct user_id,product_id,IF(order_number=MAX(order_number) OVER (PARTITION BY user_id), 1, 0) as in_cart FROM (SELECT *, MAX(order_number) OVER (PARTITION BY user_id) AS maxON FROM table) M WHERE order_number = maxON"

latest_cart_df=sqlCtx.sql(latest_cart_q)
user_prod_df=sqlCtx.sql(user_prod_q)
prod_df=sqlCtx.sql(prod_q)
user_df=sqlCtx.sql(user_q)
incart_df=sqlCtx.sql(incart_q)

join_df1=user_prod_df.join(latest_cart_df,['user_id'],"left")
join_df2=join_df1.join(prod_df,['product_id'],"left")
join_df3=join_df2.join(user_df,['user_id'],"left")
join_df4=join_df3.join(incart_df,['user_id','product_id'],"left")
join_df4=join_df4.na.fill(value=0,subset=["in_cart"])

final_df=join_df4.withColumn("user_product_order_freq", join_df4["user_product_total_orders"] / join_df4["user_total_orders"]).withColumn("product_avg_add_to_cart_order", join_df4["product_sum_add_to_cart_order"] / join_df4["product_total_orders"]).withColumn("user_avg_cartsize", join_df4["user_sum_add_to_cart_order"]/join_df4["total_orders"]).withColumn("user_product_avg_add_to_cart_order", join_df4["user_prod_sum_add_to_cart_order"] / join_df4["user_product_total_orders"]).drop("user_prod_sum_add_to_cart_order","product_sum_add_to_cart_order")

final_df.filter(final_df.user_id == 21285).show(truncate=False,n=100)
incart_df.filter(incart_df.user_id == 21285).show(truncate=False,n=100)
union_orders.filter(union_orders.product_id == 1).show(truncate=False,n=100)

def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

array_to_string_udf = udf(array_to_string, StringType())

df = final_df.withColumn('latest_cart', array_to_string_udf(final_df["latest_cart"]))

df.repartition(1).write.csv('final_data',header=True) 
