package sequencer.algorithm.approximate_match;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class ApproximateMatchSequencer implements Serializable {
    private String pattern;
    private int editLimit;

    public PairFlatMapFunction<Tuple2<Long, String>, Long, Long> ApproximateMatchMap;

    public ApproximateMatchSequencer(String p, int e) {
        this.pattern = p;
        this.editLimit = e;

        this.ApproximateMatchMap = instanceApproximateMatchMapFunc();
    }

    private PairFlatMapFunction<Tuple2<Long, String>, Long, Long> instanceApproximateMatchMapFunc() {
        return new PairFlatMapFunction<Tuple2<Long, String>, Long, Long>() {
            public Iterator<Tuple2<Long, Long>> call(Tuple2<Long, String> t){
                String sequence = t._2();
                long baseOffset = t._1();

                ArrayList<Tuple2<Long, Long>> result = new ArrayList<>();

                ApproximateMatcher am = new ApproximateMatcher(pattern, sequence);
                am.executeApproximateMatch();
                long minEditsElement = am.getMinEditsEl();
                ArrayList<Integer> offsets = am.getOffsets();
                if (minEditsElement < editLimit) {
                    for (long offset : offsets)
                        result.add(new Tuple2<>(minEditsElement, baseOffset + offset));
                }
                return result.iterator();
            }
        };
    }
}
