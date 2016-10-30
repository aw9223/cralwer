package org.forUgram.common;

public final class StringUtils {

    private StringUtils() {
    }
    
    public static String tabbedOf(int depth) {
        final StringBuilder sb = new StringBuilder();

        for (int n = 0; n < depth; ++n) {
            sb.append('\t');
        }

        return sb.toString();
    }
}
