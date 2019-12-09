package sequencer.algorithm.boyer_moore;
import java.util.ArrayList;
import java.util.Arrays;

import scala.Serializable;

class BoyerMoore implements Serializable {

    private char[] pattern;
    private int[] bad_char_rule_table;
    private int[] good_suffix_rule_table;

    BoyerMoore(String pattern) {
        this.pattern = pattern.toCharArray();

        this.bad_char_rule_table = makeBCtable();
        this.good_suffix_rule_table = makeGStable();
    }

    ArrayList<Integer> searchPattern(String sequenceString) {

        char[] sequence = sequenceString.toCharArray();
        int m = this.pattern.length;
        int n = sequence.length;

        if (m == 0)
            return null;

        ArrayList<Integer> matches = new ArrayList<>();

        for (int i = m - 1, j; i < n;) {

            j = m - 1;

            while (j >= 0 && i >= 0 && pattern[j] == sequence[i]) {
                --j;
                --i;
            }

            if (j == -1) {
                matches.add(i + 1);
                i += Math.max(good_suffix_rule_table[m - 1], 1);
            }
            else
            {
                i += Math.max(good_suffix_rule_table[m - 1 - j],
                        bad_char_rule_table[sequence[i]]);
            }
        }
        return matches;
    }

    private int[] makeBCtable() {
        final int ALPHABET_SIZE = 256;
        int[] table = new int[ALPHABET_SIZE];
        Arrays.fill(table, this.pattern.length);

        for (int i = 0; i < pattern.length; i++)
            table[pattern[i]] = pattern.length - 1 - i;

        return table;
    }

    private int[] makeGStable() {
        int n = this.pattern.length;
        int[] table = new int[n];
        int lastPrefixPosition = n;
        for (int i = n - 1; i >= 0; --i) {
            if (isPrefix(i + 1))
                lastPrefixPosition = i + 1;
            table[n - 1 - i] = lastPrefixPosition - i + n - 1;
        }

        for (int i = 0; i < n - 1; ++i) {
            int slen = suffixLength(i);
            table[slen] = n - 1 - i + slen;
        }
        return table;
    }

    private boolean isPrefix(int p) {
        int n = this.pattern.length;
        for (int i = p, j = 0; i < n; ++i, ++j)
            if (pattern[i] != pattern[j])
                return false;
        return true;
    }

    private int suffixLength(int p) {
        int n = this.pattern.length;
        int len = 0;
        for (int i = p, j = n - 1; i >= 0 && pattern[i] == pattern[j]; --i, --j)
            len += 1;
        return len;
    }

}