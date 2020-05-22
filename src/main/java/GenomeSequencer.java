import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
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
//        System.setProperty("hadoop.home.dir", "C:\\winutils");

//        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        if (args.length != 5) {
            System.out.println("usage: -input -output -algorithm -pattern -numOfPartitions (if 0, then use default)");
            return;
        }
        String inputFile = args[0];
        String output = args[1];
        String sequencerAlgorithm = args[2];
        String pattern = args[3];
        int editLimit = (int) Math.ceil(pattern.length() * 0.1);
        int scoreThreshold = (int) Math.ceil(pattern.length());

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName(sequencerAlgorithm);
//        SparkConf conf = new SparkConf().setAppName(sequencerAlgorithm).setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = new JavaSparkContext(sc);

        Broadcast<String> broadcastPattern = jsc.broadcast(pattern);
        Broadcast<Integer> broadcastEditLimit = jsc.broadcast(editLimit);
        Broadcast<Integer> broadcastScoreThreshold = jsc.broadcast(scoreThreshold);
        // Get Hadoop config
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("pattern", pattern);
        hadoopConf.setInt("patternLength", pattern.length());

        // Get hdfs object
        FileSystem hdfs = FileSystem.get(hadoopConf);


        // Set number of partitions
        final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;

        int numOfAvailableCores = getRuntime().availableProcessors()
                * Math.max((sc.statusTracker().getExecutorInfos().length - 1), 1);
//        int numOfPartitions = numOfAvailableCores * 2;
        int numOfPartitions = Integer.parseInt(args[4]);
        if (numOfPartitions != 0) {
            ContentSummary cSummary = hdfs.getContentSummary(new Path(inputFile));
            long fileSize = cSummary.getLength();
            long splitSize = fileSize / numOfPartitions;
            hadoopConf.setLong(SequenceInputFormat.SPLIT_MAXSIZE, splitSize);
        }


        if (hdfs.exists(new Path(output))) {
            hdfs.delete(new Path(output), true);
        }
        JavaPairRDD<Long, String> sequences = jsc
                .newAPIHadoopFile(inputFile, SequenceInputFormat.class, LongWritable.class, Text.class, hadoopConf)
                .mapToPair(sequence -> new Tuple2<>(sequence._1().get(), sequence._2().toString()));

        long start = System.currentTimeMillis();
        switch (sequencerAlgorithm) {
            case "index-sequencer": {
                IndexSequencer indexSequencerInstance = new IndexSequencer(broadcastPattern);
                JavaPairRDD<String, Long> k_mers = sequences.flatMapToPair(indexSequencerInstance.CreateKmers);
                JavaRDD offsets = k_mers.filter(indexSequencerInstance.PatternFilter).map(Tuple2::_2);
                offsets.saveAsTextFile(output);
                break;
            }
            case "boyer-moore-sequencer": {
                BoyerMooreSequencer functions = new BoyerMooreSequencer(broadcastPattern);
                JavaRDD offsets = sequences.flatMap(functions.BoyerMooreMap);
                offsets.saveAsTextFile(output);
                break;
            }
            case "approximate-match-sequencer": {
                ApproximateMatchSequencer functions = new ApproximateMatchSequencer(broadcastPattern, broadcastEditLimit);
//                Broadcast<ApproximateMatchSequencer> broadcastFuncs = jsc.broadcast(functions);
                JavaPairRDD<Long, Long> offsets = sequences
                        .flatMapToPair(functions.ApproximateMatchFlatMap)
//                        .flatMapToPair(broadcastFuncs.getValue().ApproximateMatchFlatMap)
//                        .filter(broadcastFuncs.getValue().SequencesFilter);
                        .filter(functions.SequencesFilter);
                offsets.saveAsTextFile(output);
                break;
            }
            case "local-alignment-sequencer": {
                LocalAlignmentSequencer functions = new LocalAlignmentSequencer(broadcastPattern, broadcastScoreThreshold);
                JavaPairRDD<Long, Long> offsets = sequences
                        .mapToPair(functions.LocalAlignmentMap)
                        .filter(functions.SequencesFilter);
                offsets.saveAsTextFile(output);
                break;
            }
        }

        jsc.close();

        long end = System.currentTimeMillis();
        long elapsed = (end - start) / 1000;
        System.out.println("Execution time in seconds: " + elapsed);
    }
}
