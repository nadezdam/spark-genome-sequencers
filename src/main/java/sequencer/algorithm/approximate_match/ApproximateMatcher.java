package sequencer.algorithm.approximate_match;

import java.util.ArrayList;

class ApproximateMatcher {
    private String pattern;
    private String sequence;
    private int[][] matrix;
    private int[][] traceback;
    private int n;
    private int m;

    private int minEditsEl;
    private ArrayList<Integer> offsets;

    private static final int DIAG = 1;
    private static final int UP = 2;
    private static final int LEFT = 3;
    private static final int ZERO = 4;

    ApproximateMatcher(String pattern, String sequence) {
        this.pattern = pattern;
        this.sequence = sequence;

        this.m = pattern.length() + 1;
        this.n = sequence.length() + 1;

        matrix = new int[m][n];
        traceback = new int[m][n];

        this.offsets = new ArrayList<>();
    }

    int getMinEditsEl() {
        return this.minEditsEl;
    }

    ArrayList<Integer> getOffsets() {
        return this.offsets;
    }

    void executeApproximateMatch() {
        this.buildMatrices();

        this.findMinEditElement();

        this.tracebackStep();
    }

    private void buildMatrices() {
        int i, j;
        // first column in matrices
        for (i = 0; i < m; i++) {
            matrix[i][0] = i;
            traceback[i][0] = ZERO;
        }
        // first row in matrices
        for (j = 0; j < n; j++) {
            matrix[0][j] = 0;
            traceback[0][j] = ZERO;
        }

        // rest of the matrices
        for (i = 1; i < m; i++) {
            for (j = 1; j < n; j++) {
                int diagScore = matrix[i - 1][j - 1] + similarity(i, j);
                int upScore = matrix[i - 1][j] + 1;
                int leftScore = matrix[i][j - 1] + 1;

                matrix[i][j] = Math
                        .min(diagScore, Math.min(upScore, leftScore));

                if (diagScore == matrix[i][j]) {
                    traceback[i][j] = DIAG;
                }
                if (upScore == matrix[i][j]) {
                    traceback[i][j] = UP;
                }
                if (leftScore == matrix[i][j]) {
                    traceback[i][j] = LEFT;
                }
                // if (0 == matrix[i][j]) {
                // traceback[i][j] = ZERO;
                // }
            }
        }
    }

    private int similarity(int i, int j) {
        return (pattern.charAt(i - 1) == sequence.charAt(j - 1)) ? 0 : 1;
    }

    private void findMinEditElement() {
        int i = m - 1;
        this.minEditsEl = Integer.MAX_VALUE;
        for (int k = 0; k < n; k++) {

            if (matrix[i][k] < this.minEditsEl) {
                this.minEditsEl = matrix[i][k];
            }
        }
    }

    private void tracebackStep() {
        int i = m - 1;
        for (int j = 0; j < n; j++) {
            if (matrix[i][j] == this.minEditsEl)
                tracebackStep(i, j, "", "");
        }
    }

    private void tracebackStep(int i, int j, String genomeAlign,
                               String patternAlign) {
        if (traceback[i][j] == ZERO) {
            if (!this.offsets.contains(j))
                this.offsets.add(j);
            System.out.println("Genome sequence: " + this.sequence);
            System.out.println("Alignment index: " + j);
            System.out.println("Number of edits: " + this.minEditsEl);
            System.out.println();
            System.out.println("Genome alignment:  " + genomeAlign);
            System.out.println("Pattern alignment: " + patternAlign);
            System.out.println();
        }
        if (traceback[i][j] == DIAG) {
            tracebackStep(i - 1, j - 1, sequence.charAt(j - 1) + genomeAlign,
                    pattern.charAt(i - 1) + patternAlign);
        }
        if (traceback[i][j] == LEFT) {
            tracebackStep(i, j - 1, sequence.charAt(j - 1) + genomeAlign, "-"
                    + patternAlign);
        }
        if (traceback[i][j] == UP) {
            tracebackStep(i - 1, j, "-" + genomeAlign, pattern.charAt(i - 1)
                    + patternAlign);
        }
    }
}
