package org.forUgram.database;

public class OverflowException extends RuntimeException {

    private final Comparable key;
    private final long newNode;

    protected OverflowException(Comparable key, long newNode) {
        super();

        this.key = key;
        this.newNode = newNode;
    }
    
    public Comparable getKey() {
        return key;
    }
    
    public long getNewNode() {
        return newNode;
    }
}
