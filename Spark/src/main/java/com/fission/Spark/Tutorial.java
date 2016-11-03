package com.fission.Spark;

//import spark.api.java.*;
//import spark.api.java.function.*;
//import spark.streaming.*;
//import spark.streaming.api.java.*;
//import twitter4j.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Tutorial {
	public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
                "Spark Count").setMaster("local"));
        JavaRDD<String> customerInputFile = sc.textFile("customers_data.txt");

        // Allow
        JavaPairRDD<String, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] customerSplit = s.split(",");
                return new Tuple2<String, String>(customerSplit[0], customerSplit[1]+","+customerSplit[2]);
            }
        }).distinct();

        JavaPairRDD<String, String> customerPairs2 = customerInputFile.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] customerSplit = s.split(",");
                return new Tuple2<String, String>(customerSplit[0], customerSplit[1]+","+customerSplit[2]);
            }
        });


        customerPairs.foreach(g ->
        {
           // if(Integer.parseInt(g._1()) < 4000003)
            System.out.println(g);
           // else {
             //   System.out.println("The number 4000003 is here");
            //}
        });

        customerPairs2.foreach(g ->
        {

                System.out.println(g);

        });

//    // Location of the Spark directory
//    String sparkHome = "/root/spark";
//    // URL of the Spark cluster
//    String sparkUrl = TutorialHelper.getSparkUrl();
//    // Location of the required JAR files
//    String jarFile = "target/scala-2.9.3/tutorial_2.9.3-0.1-SNAPSHOT.jar";
//    // HDFS directory for checkpointing
//    String checkpointDir = TutorialHelper.getHdfsUrl() + "/checkpoint/";
//    // Twitter credentials from login.txt
//    TutorialHelper.configureTwitterCredentials()
//    // Your code goes here
  }
}
