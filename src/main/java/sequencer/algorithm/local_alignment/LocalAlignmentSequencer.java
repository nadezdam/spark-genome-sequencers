package sequencer.algorithm.local_alignment;

import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

public class LocalAlignmentSequencer implements Serializable {
    private String pattern;
    private int scoreLimit;

    public PairFunction<Tuple2<Long, String>, Long, Long> LocalAlignmentMap;

    public LocalAlignmentSequencer(String p, int s) {
        this.pattern = p;
        this.scoreLimit = s;

        this.LocalAlignmentMap = LocalAlignmentMapFunc();
    }

    private PairFunction<Tuple2<Long, String>, Long, Long> LocalAlignmentMapFunc() {
        return new PairFunction<Tuple2<Long, String>, Long, Long>() {
            public Tuple2<Long, Long> call(Tuple2<Long, String> t) {
                String sequence = t._2();
                long base_offset = t._1();
                Tuple2<Long, Long> result = new Tuple2<>((long) -1, base_offset + (long) -1);
                SmithWaterman aligner = new SmithWaterman(sequence, pattern);
                aligner.executeSmithWatermanAlgorithm();

                int offset = aligner.getOffset();
                int score = aligner.getScore();
                if (score > scoreLimit)
                    result = new Tuple2<>((long) score, base_offset + (long) offset);
                return result;

            }
        };
    }
}
