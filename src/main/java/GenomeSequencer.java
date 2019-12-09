import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import sequence.input.format.SequenceInputFormat;
import sequencer.algorithm.approximate_match.ApproximateMatchSequencer;
import sequencer.algorithm.boyer_moore.BoyerMooreSequencer;
import sequencer.algorithm.index.IndexSequencer;
import sequencer.algorithm.local_alignment.LocalAlignmentSequencer;

import java.io.IOException;

import static java.lang.Runtime.*;

public class GenomeSequencer {
    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "C:\\winutils");
        String inputFile = args[0];
        String output = args[1];
        String pattern = args[2];
        int editLimit = Integer.parseInt(args[3]);
        int scoreLimit = Integer.parseInt(args[4]);
        String sequencerAlgorithm = args[5];

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName(sequencerAlgorithm).setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);

        int numOfPartitions = getRuntime().availableProcessors()
                * Math.max((sc.statusTracker().getExecutorInfos().length - 1), 1);
        numOfPartitions = 4;
        JavaSparkContext jsc = new JavaSparkContext(sc);
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("pattern", pattern);
        hadoopConf.setInt("patternLength", pattern.length());
        FileSystem fs = FileSystem.get(hadoopConf);

        if (fs.exists(new Path(output))) {
            fs.delete(new Path(output), true);
        }
        JavaPairRDD<Long, String> sequences = jsc
                .newAPIHadoopFile(inputFile, SequenceInputFormat.class, LongWritable.class, Text.class, hadoopConf)
                .mapToPair(sequence -> new Tuple2<>(sequence._1().get(), sequence._2().toString()));
//                .repartition(numOfPartitions);

        long start = System.currentTimeMillis();
        switch (sequencerAlgorithm) {
            case "index-sequencer": {
                IndexSequencer indexSequencerInstance = new IndexSequencer(pattern);
                JavaPairRDD<String, Long> k_mers = sequences.flatMapToPair(indexSequencerInstance.Kmers);
                JavaRDD offsets = k_mers.filter(indexSequencerInstance.PatternFilter).map(Tuple2::_2);
                offsets.saveAsTextFile(output);
                break;
            }
            case "boyer-moore-sequencer": {
                BoyerMooreSequencer functions = new BoyerMooreSequencer(pattern);
                JavaRDD offsets = sequences.flatMap(functions.BoyerMooreMap);
                offsets.saveAsTextFile(output);
                break;
            }
            case "approximate-match-sequencer": {
                ApproximateMatchSequencer functions = new ApproximateMatchSequencer(pattern, editLimit);
                JavaPairRDD<Long, Long> offsets = sequences.flatMapToPair(functions.ApproximateMatchMap);
                offsets.saveAsTextFile(output);
                break;
            }
            case "local-alignment-sequencer": {
                LocalAlignmentSequencer funcs = new LocalAlignmentSequencer(pattern, scoreLimit);
                JavaPairRDD<Long, Long> offsets = sequences.mapToPair(funcs.LocalAlignmentMap);
                offsets.saveAsTextFile(output);
                break;
            }
        }


//        System.out.println("Num of partitions:" + offsets.getNumPartitions());
//        offsets.persist(StorageLevel.MEMORY_ONLY());
//        System.out.println("Num of partitions:" + offsets.getNumPartitions());

        jsc.close();

        long end = System.currentTimeMillis();
        long elapsed = (end - start) / 1000;
        System.out.println("Elapsed time in seconds: " + elapsed);
    }

    private void executeSequencing(JavaPairRDD<Long, String> sequences, String sequenceAlgorithm) {
    }
}
