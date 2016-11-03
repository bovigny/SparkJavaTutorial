package com.fission.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class EndingWith {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster(
				"local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> listRdd = context.parallelize(Arrays.asList("chanti",
				"ganesh", "naresh", "ritesh", "fission_labs", "value_labs",
				"pega systems"));
		System.out.println("ListRDD before calling an action: " + listRdd);
		System.out.println("HERE THE FIRST NAME OF THE LIST"+ listRdd.first());

        JavaRDD<String> test = listRdd.filter(it -> {
            if(it.startsWith("cha")){
                return true;
            } else {
                return false;
            }
        });

        System.out.println(test.collect().get(0));



		JavaRDD<String> endingWithSh = listRdd
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String item) throws Exception {
						if (item.endsWith("sh"))
							return true;
						return false;
					}
				});

		System.out.println(endingWithSh.collect());

		JavaRDD<String> endingWithLabs = listRdd
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String item) throws Exception {
						if (item.endsWith("labs"))
							return true;
						return false;
					}
				});

		System.out.println(endingWithLabs.collect());
		context.close();
	}
}