package com.appranix.spark;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Counts the number of words in the given file
 */
public class WordCount  {


    public static void wordCountJava8(String filename, String url, String username, String password ) throws SQLException {
    	
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count App");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data, which is a text file read from the command line
        JavaRDD<String> input = sc.textFile( filename );

        // Java 8 with lambdas: split the input string into words
          JavaRDD<String> words = input.flatMap( s -> Arrays.asList( s.split( " " ) ).iterator() );

        // Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
        JavaPairRDD<String, Integer> counts = words.mapToPair( t -> new Tuple2( t, 1 ) ).reduceByKey( (x, y) -> (int)x + (int)y );
        

    	Redshift red = new Redshift(url, username, password);
    	red.createWordCountTable();
    	
    	Map<String, Integer> collectionsMap = counts.collectAsMap();

        for(String key: collectionsMap.keySet()) {
        	
        	//System.out.println("Word = " + key + " / count = " + collectionsMap.get(key));
        	try {
				red.insertWordCount(key, collectionsMap.get(key));
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
	        
	        
//        }    
//        else if( outputType.equals("1") ) {
//        	// Save the word count back out to a text file, causing evaluation.
//        	counts.saveAsTextFile( output );
//        }	
    }
       

    public static void main( String[] args ) throws Exception {
        
    	if( args.length < 4 ) {
            System.out.println( "Invalid arguments.." );
            throw new Exception("Invalid arguments. Please correct and try again.");
        }
        
		wordCountJava8( args[ 0 ], args[ 1 ],  args[ 2 ], args[ 3 ] );
 
        
    }
}
