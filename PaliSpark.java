
//http://spark.apache.org/examples.html

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.io.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class PaliSpark  {
    private static final Pattern space = Pattern.compile(" ");

       public static void main(String[] args) throws IOException {
        

        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        sparkContext.hadoopConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        JavaRDD<String> inputfile = sparkContext.textFile(args[0]);
        String outputPath = args[1]+"/Apache/Spark/"+(new Date()).getTime();

        JavaRDD<String> mWords = inputfile.flatMap(s -> Arrays.asList(space.split(s.trim().toLowerCase())).iterator());

        JavaRDD<String> dictonaryWords = mWords.flatMap(s -> Arrays.asList(s.trim().replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "")).iterator());

        JavaPairRDD<String, Integer> ones = dictonaryWords.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.filter(s->PaliSpark.checkPalindrome(s._1)).sortByKey(true).reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
                                              
        counts.saveAsTextFile(outputPath);

        System.out.println("Spark Palindrome Job!");
        
        for (Tuple2<?,?> tuple : output) {
             
        
            if(tuple._1().toString().length()>4) 
            {
                System.out.println("Palindromes = "+tuple._1() + " , Count " + tuple._2());
            }
        }

}

 public static boolean checkPalindrome(String str){
        return str.equals(new StringBuilder(str).reverse().toString());
    }

}