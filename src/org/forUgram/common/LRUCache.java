package org.forUgram.common;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class LRUCache<K, V> extends LinkedHashMap<K, V> {

    private final int cacheSize;

    public LRUCache(final int cacheSize) {
        super(cacheSize + 1, 0.75f, true);
        this.cacheSize = cacheSize;
    }

    protected abstract void entryRemoved(final Map.Entry<K, V> eldest);

    @Override
    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        if (size() > cacheSize) {
            entryRemoved(eldest);
            return true;
        }

        return false;
    }
}
