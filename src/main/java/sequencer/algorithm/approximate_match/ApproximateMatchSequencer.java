package sequencer.algorithm.approximate_match;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class ApproximateMatchSequencer implements Serializable {
    private String pattern;
    private int editLimit;

    public PairFlatMapFunction<Tuple2<Long, String>, Long, Long> ApproximateMatchFlatMap;
    public Function<Tuple2<Long, Long>, Boolean> SequencesFilter;

    public ApproximateMatchSequencer(Broadcast<String> p, Broadcast<Integer> e) {
        this.pattern = p.getValue();
        this.editLimit = e.getValue();

        this.ApproximateMatchFlatMap = instanceApproximateMatchFlatMapFunc();
        this.SequencesFilter = instanceSequencesFilterFunc();
    }

    private PairFlatMapFunction<Tuple2<Long, String>, Long, Long> instanceApproximateMatchFlatMapFunc() {
        return new PairFlatMapFunction<Tuple2<Long, String>, Long, Long>() {
            public Iterator<Tuple2<Long, Long>> call(Tuple2<Long, String> t) {
                String sequence = t._2();
                long baseOffset = t._1();

                ArrayList<Tuple2<Long, Long>> result = new ArrayList<>();

                ApproximateMatcher am = new ApproximateMatcher(pattern, sequence);
                am.executeApproximateMatch();
                long minEditsElement = am.getMinEditsEl();
                ArrayList<Integer> offsets = am.getOffsets();
//                if (minEditsElement < editLimit) {
                for (long offset : offsets)
                    result.add(new Tuple2<>(minEditsElement, baseOffset + offset));
//                }
                return result.iterator();
            }
        };
    }

    private Function<Tuple2<Long, Long>, Boolean> instanceSequencesFilterFunc() {
        return new Function<Tuple2<Long, Long>, Boolean>() {
            public Boolean call(Tuple2<Long, Long> value) {
                long numOfEdits = value._1();
                return numOfEdits < editLimit;
            }
        };
    }
}
