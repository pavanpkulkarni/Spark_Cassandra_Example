package com.pavanpkulkarni.cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer._

object SparkCassandra {

    case class students_cc(id : Int, year_graduated : String, courses_registered : List[cid_sem], name : String)
    case class cid_sem(cid : String, sem : String)

    def main(args : Array[String]) : Unit = {
        //Start the Spark context

        val spark = SparkSession
            .builder()
            .master("local")
            .master("local")
            .appName("Spark_Cassandra")
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()

        import spark.implicits._

        // Read values from Cassandra

        val studentsDF = spark
            .read
            .cassandraFormat("students", "test_keyspace")
            .options(ReadConf.SplitSizeInMBParam.option(32))
            .load()

        studentsDF.show(false)

        studentsDF.printSchema()

        val coursesDF = spark
            .read
            .cassandraFormat("courses", "test_keyspace")
            .options(ReadConf.SplitSizeInMBParam.option(32))
            .load()

        println("Courses are : ")
        coursesDF.show()

        // Write a row to Cassandra
        val newCourses = Seq(Row("CS011", "ML - Advance"), Row("CS012", "Sentiment Analysis"))
        val courseSchema = List(StructField("cid", StringType, true), StructField("cname", StringType, true))
        val newCoursesDF = spark.createDataFrame(spark.sparkContext.parallelize(newCourses), StructType(courseSchema))

        newCoursesDF.write
            .mode(SaveMode.Append)
            .cassandraFormat("courses", "test_keyspace")
            .save()

        val readNewCourse = spark
            .read
            .cassandraFormat("courses", "test_keyspace")
            .options(ReadConf.SplitSizeInMBParam.option(32))
            .load()
            .show()

        // Write UDT to Cassandra.

        val listOfCoursesSem = List(cid_sem("CS003", "Spring_2011"),
            cid_sem("CS006", "Summer_2011"),
            cid_sem("CS009", "Fall_2011")
        )

        val newStudents = Seq(students_cc(12, "2011", listOfCoursesSem, "Black Panther"))

        val newStudetntsRDD = spark.sparkContext.parallelize(newStudents)
        val newStudentsDF = spark.createDataFrame(newStudetntsRDD)

        newStudentsDF.write
            .mode(SaveMode.Append)
            .cassandraFormat("students", "test_keyspace")
            .save()

        val readNewStudents = spark
            .read
            .cassandraFormat("students", "test_keyspace")
            .options(ReadConf.SplitSizeInMBParam.option(32))
            .load()
            .show()

        //Use Case : Get the names of students, courses taken, semester, course id, year_graudated.

        val flatData = studentsDF.withColumn("flatDataCol", explode($"courses_registered"))
            .withColumn("cid", $"flatDataCol".getItem("cid"))
            .withColumn("sem", $"flatDataCol".getItem("sem"))
            .drop("flatDataCol")
            .drop("courses_registered")

        flatData.show(false)

        //join coursesDF with flatData on col("cid")

        val result = flatData.join(coursesDF, Seq("cid"))
            .select("name", "cname", "sem", "cid", "year_graduated")

        result.show(false)

    }
}