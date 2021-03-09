package com.cloudera.examples
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType

import scala.io.Source

object KafkaStreamPrintConsole {

  def main(args: Array[String]): Unit = {

   /* val ksourcetopic = args(0)
    val kbrokers = args(1)
    val startingOffsets = args(2) //ie latest
    val s3bucket = args(3)
    val checkpoint = args(4)  //ie s3 bucket
    val zkUrl= args(5)
    val lookupTblName= args(6)
    val ktargettopic = args(7) */

       val ksourcetopic = "moad-covid-json"
    val kbrokers = "moad-aw-dev0-dataflow-worker0.moad-aw.yovj-8g7d.cloudera.site:9093,moad-aw-dev0-dataflow-worker2.moad-aw.yovj-8g7d.cloudera.site:9093,moad-aw-dev0-dataflow-worker1.moad-aw.yovj-8g7d.cloudera.site:9093"
    val startingOffsets = "earliest"
    //val s3bucket = args(3)
    val checkpoint ="/tmp/spark-checkpoint1/"  //ie s3 bucket
    val zkUrl= "cod--or6q9la130qm-leader0.moad-aw.yovj-8g7d.cloudera.site,cod--or6q9la130qm-master0.moad-aw.yovj-8g7d.cloudera.site,cod--or6q9la130qm-master1.moad-aw.yovj-8g7d.cloudera.site:2181"
    val lookupTblName= "country_lookup"
    val ktargettopic = "moad-covid-json-merged"
    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("source topic: "+ksourcetopic)
    println("brokers: "+kbrokers)
    println("startingOffsets: "+startingOffsets)
   // println("s3bucket: "+s3bucket)
    println("checkpoint: "+checkpoint)
    println("\n*******************************")
    println("\n*******************************")

/* FOR CDE
    val spark = SparkSession.builder
      .appName("Spark Kafka Secure Structured Streaming Example")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("spark.kafka.sasl.mechanism", "PLAIN")
      .config("spark.kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .config("spark.sql.streaming.checkpointLocation", "/app/mount/spark-checkpoint5")
      .getOrCreate() 
 *      
 */
      
      val spark = SparkSession.builder
      .appName("Spark Kafka Secure Structured Streaming Example")
      .master("local")
      .config("spark.kafka.bootstrap.servers", kbrokers)
      .config("spark.kafka.sasl.kerberos.service.name", "kafka")
      .config("spark.kafka.security.protocol", "SASL_SSL")
      .config("kafka.sasl.mechanism", "PLAIN")
      .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=./src/main/resources/jaas.conf")
      .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=./src/main/resources/jaas.conf")
      .config("spark.kafka.ssl.truststore.location", "./src/main/resources/truststore.jks")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    val phoenixConnector= new PhoenixConnector(spark,zkUrl,lookupTblName)
    import spark.implicits._

    //get schema and broadcast
    val jsonStr = covidschema.coviddata
    val covidDataScheme = spark.read.json(Seq(jsonStr).toDS).toDF().schema
    covidDataScheme.printTreeString()
    val broadcastSchema = spark.sparkContext.broadcast(covidDataScheme)

    //read moad stream
    val dfreadstream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kbrokers)
      .option("subscribe", ksourcetopic)
      .option("startingOffsets", startingOffsets)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      //.option("kafka.ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts")
      .option("kafka.ssl.truststore.location", "./src/main/resources/truststore.jks")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("failOnDataLoss", "false")
      .option("checkpointLocation", checkpoint)
      .load()

      
 val query = dfreadstream.writeStream
  .outputMode("append")
  .format("console")
  .start()
query.awaitTermination()


  }


}