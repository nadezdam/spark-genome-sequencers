package sequencer.algorithm.index;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class IndexSequencer implements Serializable {
    private String pattern;
    private int patternLength;

    public PairFlatMapFunction<Tuple2<Long, String>, String, Long> CreateKmers;
    public Function<Tuple2<String, Long>, Boolean> PatternFilter;

    public IndexSequencer(Broadcast<String> pattern) {
        this.pattern = pattern.getValue();
        this.patternLength = this.pattern.length();

        this.CreateKmers = instanceCreateKMersFunc();
        this.PatternFilter = instancePatternFilterFunc();
    }

    private PairFlatMapFunction<Tuple2<Long, String>, String, Long> instanceCreateKMersFunc() {
        return new PairFlatMapFunction<Tuple2<Long, String>, String, Long>() {
            public Iterator<Tuple2<String, Long>> call(Tuple2<Long, String> tuple) {
                ArrayList<Tuple2<String, Long>> kmers = new ArrayList<>();
                Long offset = tuple._1();
                String sequence = tuple._2();

                for (int i = 0; i < sequence.length() - patternLength + 1; i++) {
                    String subsequence = sequence.substring(i, i + patternLength);
                    long subseqOffset = offset + i;
                    kmers.add(new Tuple2<>(subsequence, subseqOffset));
                }
                return kmers.iterator();
            }
        };
    }

    private Function<Tuple2<String, Long>, Boolean> instancePatternFilterFunc() {
        return new Function<Tuple2<String, Long>, Boolean>() {
            public Boolean call(Tuple2<String, Long> value) {
                return value._1().equalsIgnoreCase(pattern);
            }
        };
    }

}
