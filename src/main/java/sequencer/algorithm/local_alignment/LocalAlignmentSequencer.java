package sequencer.algorithm.local_alignment;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

public class LocalAlignmentSequencer implements Serializable {
    private String pattern;
    private int scoreThreshold;

    public PairFunction<Tuple2<Long, String>, Long, Long> LocalAlignmentMap;
    public Function<Tuple2<Long, Long>, Boolean> SequencesFilter;

    public LocalAlignmentSequencer(Broadcast<String> p, Broadcast<Integer> s) {
        this.pattern = p.getValue();
        this.scoreThreshold = s.getValue();

        this.LocalAlignmentMap = instanceLocalAlignmentMapFunc();
        this.SequencesFilter = instanceSequencesFilterFunc();
    }

    private PairFunction<Tuple2<Long, String>, Long, Long> instanceLocalAlignmentMapFunc() {
        return new PairFunction<Tuple2<Long, String>, Long, Long>() {
            public Tuple2<Long, Long> call(Tuple2<Long, String> t) {
                String sequence = t._2();
                long base_offset = t._1();
                SmithWaterman aligner = new SmithWaterman(sequence, pattern);
                aligner.executeSmithWatermanAlgorithm();

                int offset = aligner.getOffset();
                int score = aligner.getScore();
                return new Tuple2<>((long) score, base_offset + (long) offset);

            }
        };
    }

    private Function<Tuple2<Long, Long>, Boolean> instanceSequencesFilterFunc() {
        return new Function<Tuple2<Long, Long>, Boolean>() {
            public Boolean call(Tuple2<Long, Long> value) {
                long scores = value._1();
                return scores > scoreThreshold;
            }
        };
    }
}
