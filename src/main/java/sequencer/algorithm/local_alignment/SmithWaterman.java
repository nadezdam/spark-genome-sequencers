package sequencer.algorithm.local_alignment;

import scala.Serializable;

class SmithWaterman implements Serializable {

    private String genomeSequence, pattern;
    // aligned substring
    private String genomeMatch, patternMatch;
    private int n, m;
    private int[][] scoreMatrix;
    private int[][] tracebackMatrix;

    private int score;
    private int offset;
    // scores and penalties
    private static final int MATCH = 2;
    private static final int MISMATCH = -2;
    private static final int GAP = -3;

    // directions in traceback matrix
    private static final int DIAG = 1;
    private static final int UP = 2;
    private static final int LEFT = 3;
    private static final int ZERO = 4;

    int getScore(){
    	return this.score;
	}

	int getOffset(){
    	return this.offset;
	}

    SmithWaterman(String genomeSeq, String patternSeq) {
        this.genomeSequence = genomeSeq;
        this.pattern = patternSeq;

        this.n = genomeSequence.length() + 1;
        this.m = pattern.length() + 1;

        scoreMatrix = new int[m][n];
        tracebackMatrix = new int[m][n];

        this.genomeMatch = "";
        this.patternMatch = "";
    }

    void executeSmithWatermanAlgorithm() {

        buildMatrices();
        this.score = tracebackStep();
    }

    private void buildMatrices() {
        int i, j;
        // first column in matrices
        for (i = 0; i < m; i++) {
            scoreMatrix[i][0] = 0;
            tracebackMatrix[i][0] = ZERO;
        }
        // first row in matrices
        for (j = 0; j < n; j++) {
            scoreMatrix[0][j] = 0;
            tracebackMatrix[0][j] = ZERO;
        }

        // rest of the matrices
        for (i = 1; i < m; i++) {
            for (j = 1; j < n; j++) {
                int diagScore = scoreMatrix[i - 1][j - 1] + similarity(i, j);
                int upScore = scoreMatrix[i - 1][j] + similarity(i, -1);
                int leftScore = scoreMatrix[i][j - 1] + similarity(-1, j);

                scoreMatrix[i][j] = Math.max(diagScore,
                        Math.max(upScore, Math.max(leftScore, 0)));

                if (diagScore == scoreMatrix[i][j]) {
                    tracebackMatrix[i][j] = DIAG;
                }
                if (upScore == scoreMatrix[i][j]) {
                    tracebackMatrix[i][j] = UP;
                }
                if (leftScore == scoreMatrix[i][j]) {
                    tracebackMatrix[i][j] = LEFT;
                }
                if (0 == scoreMatrix[i][j]) {
                    tracebackMatrix[i][j] = ZERO;
                }
            }
        }
    }

    private int tracebackStep() {
        int maxScore = getMaxScore();
        int maxI = 1, maxJ = 1;
        for (int i = 1; i < m; i++) {
            for (int j = 1; j < n; j++) {
                if (scoreMatrix[i][j] == maxScore) {
                    maxI = i;
                    maxJ = j;
                }
            }
        }

        getAlignments(maxI, maxJ, "", "");
        boolean correctScore = checkScore(maxScore);

        if (correctScore) {
            System.out.println("Maximum score in matrix: " + maxScore);
            System.out.println();
        }
        return correctScore ? maxScore : 0;

    }

    private int similarity(int i, int j) {
        if (i == -1 || j == -1) {
            return GAP;
        }

        return (genomeSequence.charAt(j - 1) == pattern.charAt(i - 1)) ? MATCH
                : MISMATCH;
    }

    private int getMaxScore() {
        int max = 0;

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                if (scoreMatrix[i][j] > max)
                    max = scoreMatrix[i][j];
            }
        }
        return max;
    }

    private void getAlignments(int i, int j, String alignment1,
                               String alignment2) {
        if (tracebackMatrix[i][j] == ZERO) {

            this.genomeMatch = alignment1;
            this.patternMatch = alignment2;

            System.out.println("Genome match sequence:  " + genomeMatch);
            System.out.println("Pattern match sequence: " + patternMatch);
            System.out.println();

            this.offset = j;
        }

        if (tracebackMatrix[i][j] == DIAG) {
            getAlignments(i - 1, j - 1, genomeSequence.charAt(j - 1)
                    + alignment1, pattern.charAt(i - 1) + alignment2);
        }
        if (tracebackMatrix[i][j] == LEFT) {
            getAlignments(i, j - 1, genomeSequence.charAt(j - 1) + alignment1,
                    "-" + alignment2);
        }
        if (tracebackMatrix[i][j] == UP) {
            getAlignments(i - 1, j, "-" + alignment1, pattern.charAt(i - 1)
                    + alignment2);
        }
    }

    private boolean checkScore(int maxScore) {
        int score = 0;

        for (int i = 0; i < genomeMatch.length(); i++) {
            if (genomeMatch.charAt(i) == patternMatch.charAt(i)) {
                score += MATCH;
                continue;
            }
            if (genomeMatch.charAt(i) == '-' || patternMatch.charAt(i) == '-') {
                score += GAP;
                continue;
            }
            if (genomeMatch.charAt(i) != patternMatch.charAt(i)) {
                score += MISMATCH;
            }
        }

        return score == maxScore;
    }
}
