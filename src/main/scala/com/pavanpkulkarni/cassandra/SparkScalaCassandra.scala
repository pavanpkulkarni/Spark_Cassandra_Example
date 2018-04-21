package com.pavanpkulkarni.cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
//import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf


object SparkCassandra {
	def main(args: Array[String]): Unit = {
			//Start the Spark context

			val spark = SparkSession
					.builder()
					.master("local")
					.appName("Spark_Cassandra")
					.config("spark.cassandra.connection.host", "localhost")
					.getOrCreate()

					import spark.implicits._

					val df = spark
					.read
					.cassandraFormat("students", "test_keyspace")
					.options(ReadConf.SplitSizeInMBParam.option(32))
					.load()
					
					println("schema is : ", df.printSchema())
				

	}
}