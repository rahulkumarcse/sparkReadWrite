package co.m.rahul
package basepackage

import org.apache.spark.sql.{DataFrame, SparkSession}

object Runner extends App {

  import java.nio.file.Paths

//  static {
//    val OS = System.getProperty("os.name").toLowerCase();
//
//    if (OS.contains("win"))  else {
//      System.setProperty("hadoop.home.dir", "/");
//    }
//  }mapreduce.fileoutputcommitter.marksuccessfuljobs
  //
 System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession.builder().
    master("local[*]")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs",false)
    .appName("Sparkrunner")
   // .config("hadoop.home.dir",Paths.get("winutils").toAbsolutePath().toString())
    .getOrCreate()
  val customer = spark.read.format("csv").option("header","true").csv("src/main/resources/customer.csv")
  val purchase = spark.read.format("csv").option("header","true").csv("src/main/resources/purchase.csv")
  customer.createOrReplaceTempView("customer")
  purchase.createOrReplaceTempView("purchase")
  val result1= spark.sql("select customer.customer_id, customer.first_name, customer.last_name, purchase.product_name from customer join purchase on customer.customer_id=purchase.customer_id order by customer.customer_id")
  val result2 = spark.sql("select customer.customer_id, customer.first_name, customer.last_name,count (purchase.order_id) from customer join purchase on customer.customer_id=purchase.customer_id group by customer.customer_id, customer.first_name, customer.last_name order by customer.customer_id")
  result1.write.format("csv").option("header","true").csv("src/main/resources/results/result1")
  result2.write.format("csv").option("header","true").csv("src/main/resources/results/result2")
print("")
}
