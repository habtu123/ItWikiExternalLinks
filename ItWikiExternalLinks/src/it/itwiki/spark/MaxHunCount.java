package it.itwiki.spark;



import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.io.Serializable;

public class MaxHunCount {

public static void main(String[] args) throws Exception {
      
     
//      System.out.println("args[0]: <input-path>="+args[0]);
//      System.out.println("args[1]: <output-path>="+args[1]);
//      final String inputPath = args[0];
//      final String outputPath = args[1];
     
      SparkConf conf = new SparkConf().setAppName("App");
      JavaSparkContext  ctx = new JavaSparkContext(conf);

      JavaRDD<String> lines = (JavaRDD<String>) ctx.textFile("/output/1/", 1).collect();


      
      JavaPairRDD<String,Integer> kv = lines.mapToPair(new PairFunction<String,String,Integer>() {
         
          public Tuple2<String,Integer> call(String s) {
             String[] tokens = s.split(","); // url,789
             return new Tuple2<String,Integer>(tokens[2], Integer.parseInt(tokens[1]));
          }
       });
   //   kv.saveAsTextFile("/spartOutone");
    JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(new Function2<Integer,Integer,Integer>(){
//
		@Override
		public Integer call(Integer i1, Integer i2) throws Exception {
			// TODO Auto-generated method stub
		return i1+i2;
		}
		});
//

     List<Tuple2<String, Integer>> topNResult = uniqueKeys.takeOrdered(10, MyTupleComparator.INSTANCE);
//
     
      for (Tuple2<String, Integer> entry : topNResult) {    	  
         System.out.println(entry._2 + "--" + entry._1);
     }
     
      System.exit(0);
   }

 static class MyTupleComparator implements Comparator<Tuple2<String, Integer>> ,Serializable {

	private static final long serialVersionUID = 1L;
	final static MyTupleComparator INSTANCE = new MyTupleComparator();
       @Override
       public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
          return -t1._2.compareTo(t2._2);  
          // return t1._2.compareTo(t2._2);   // sorts RDD elements ascending (use for Bottom-N)
       }
   }
}
