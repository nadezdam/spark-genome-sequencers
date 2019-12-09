package sequencer.algorithm.boyer_moore;

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class BoyerMooreSequencer implements Serializable {
    private BoyerMoore worker;

    public FlatMapFunction<Tuple2<Long, String>, Long> BoyerMooreMap;

    public BoyerMooreSequencer(String pattern) {
        worker = new BoyerMoore(pattern);
        BoyerMooreMap = instanceBoyerMooreMapFunc();
    }

    private FlatMapFunction<Tuple2<Long, String>, Long> instanceBoyerMooreMapFunc() {
        return new FlatMapFunction<Tuple2<Long, String>, Long>() {
            @Override
            public Iterator<Long> call(Tuple2<Long, String> t) {
                long offset = t._1();
                String pattern = t._2();
                ArrayList<Integer> matches = worker.searchPattern(pattern);
                ArrayList<Long> matchesWithOffset = new ArrayList<>();

                for (Integer match : matches) {
                    matchesWithOffset.add(match + offset);
                }
                return matchesWithOffset.iterator();
            }
        };
    }
}
