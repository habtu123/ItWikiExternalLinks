package it.itwiki.spark;


// STEP-0: import required classes and interfaces

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import java.util.List;
import java.util.Comparator;
import java.io.Serializable;

/**
 * Assumption: for all input (K, V), K's are non-unique.
 * This class implements Top-N design pattern for N > 0.
 * The main assumption is that for all input (K, V)'s, K's
 * are non-unique. It means that you will find entries like
 * (A, 2), ..., (A, 5),...
 *
 * This is a general top-N algorithm which will work unique
 * and non-unique keys.
 *
 * This class may be used to find bottom-N as well (by
 * just keeping N-smallest elements in the set.
 *
 *  Top-10 Design Pattern: �Top Ten� Structure
 *
 *  1. map(input) => (K, V)
 *
 *  2. reduce(K, List<V1, V2, ..., Vn>) => (K, V),
 *                where V = V1+V2+...+Vn
 *     now all K's are unique
 *
 *  3. Find Top-N using the following high-level Spark API:
 *     java.util.List<T> takeOrdered(int N, java.util.Comparator<T> comp)
 *     Returns the first N elements from this RDD as defined by the specified
 *     Comparator[T] and maintains the order.
 *
 * @author Mahmoud Parsian
 *
 */
public class MaxHunCount implements Serializable {

public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length != 2) {
         System.err.println("Usage: Top100UsingTakeOrdered <input-path> <topN>");
         System.exit(1);
      }
      System.out.println("args[0]: <input-path>="+args[0]);
      System.out.println("args[1]: <topN>="+args[1]);
      final String inputPath = args[0];

      // STEP-2: create a Java Spark Context object
    //  JavaSparkContext ctx = SparkUtil.createJavaSparkContext("local[*]","Max 100 Count");
//      
      SparkConf conf = new SparkConf().setAppName("App");
      JavaSparkContext  ctx = new JavaSparkContext(conf);
     

      //    input record format:
      //        <string-key><,><integer-value-count>
//      JavaRDD<String> lines = ctx.textFile(inputPath, 1);
//      lines.saveAsTextFile("/output/1");
 
      
      JavaRDD<String> lines = ctx.textFile(inputPath, 1);
      lines.saveAsTextFile("/"+args[1]+"/1");
      
      
      // public JavaRDD<T> coalesce(int numPartitions)
      // Return a new RDD that is reduced into numPartitions partitions.
      JavaRDD<String> rdd = lines.coalesce(9);
//
//      // STEP-5: map input(T) into (K,V) pair
//      // PairFunction<T, K, V>
//      // T => Tuple2<K, V>
      JavaPairRDD<String,Integer> kv = rdd.mapToPair(s -> {
	    String[] tokens = s.split(","); // url,789
	    return new Tuple2<String,Integer>(tokens[2], 1);
      });
      kv.saveAsTextFile("/"+args[1]+"/2");
//
//      // STEP-6: reduce frequent K's
      JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(new Function2<Integer,Integer,Integer>(){

		@Override
		public Integer call(Integer i1, Integer i2) throws Exception {
			// TODO Auto-generated method stub
			return i1+i2;
		}
    	  
      });
     uniqueKeys.saveAsTextFile("/"+args[1]+"/3");
//
//      // STEP-7: find final top-N by calling takeOrdered()
     List<Tuple2<String, Integer>> topNResult = uniqueKeys.takeOrdered(10, MyTupleComparator.INSTANCE);
//
//      // STEP-8: emit final top-N
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
          return -t1._2.compareTo(t2._2);     // sorts RDD elements descending (use for Top-N)
          // return t1._2.compareTo(t2._2);   // sorts RDD elements ascending (use for Bottom-N)
       }
   }
}
