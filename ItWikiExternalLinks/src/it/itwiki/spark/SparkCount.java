package it.itwiki.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.actors.threadpool.Arrays;

public class SparkCount {

	public static void main (String[] args){
	
		SparkConf conf = new SparkConf().setAppName("App");
	      JavaSparkContext  sc = new JavaSparkContext(conf);
	try {
	JavaRDD<String> textFile = sc.textFile("/output1/part-m-00000");
	JavaPairRDD<String, Integer> counts = textFile
		    .flatMap(s -> Arrays.asList(s.split(",")).iterator())
		    .mapToPair(word -> new Tuple2<>(word, 1));
		counts.saveAsTextFile("/output11");
	}catch(Exception e) {
		System.out.println(e);
	}
	
	}
}
