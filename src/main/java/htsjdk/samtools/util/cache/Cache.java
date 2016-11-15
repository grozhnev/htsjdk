package htsjdk.samtools.util.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Concurrency effective universal cache structure. It represents an abstraction of infinite cell structure.
 * The {@link Cell} mapped to every key can be grabbed. {@link Cell} stores or doesn't store the value. Only one thread
 * can load value to the {@link Cell} by taking its {@link Cell.Slot} which represents an abstraction of write access.
 * Each {@link Cell} has only one {@link Cell.Slot} to be taken.
 * @see Cell
 * @see Cell.Slot
 * @param <K> - the type of the keys used to access getValue within this cache
 * @param <V> - the type of the values held within this cache
 */
public class Cache<K, V> {

    protected AtomicInteger availableSpace;

    protected final Map<K, Cell<V>> cache;

    private final Class<K> keyType;
    private final Class<V> valueType;

    Cache(Class<K> keyType, Class<V> valueType, int space) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.availableSpace = new AtomicInteger(space);
        cache = new LinkedHashMap<>();
    }

    public int getCurrentSize() {
        return cache.size();
    }

    /**
     * Retrieve the {@link Cell} currently mapped to the provided key if one is known,
     * otherwise instance a new {@link Cell}. So it always returns a {@link Cell}.
     * @param key - the key to query the {@link Cell} for
     * @return the {@link Cell} mapped to the key
     */
    public synchronized Cell<V> grabCell(K key) {
        if (!cache.containsKey(key)) {
            allocCell(key);
        }

        Cell<V> cell = cache.remove(key);
        cache.put(key, cell);

        return cell;
    }

    //not synchronized, use only from synchronized
    private void allocCell(K cellIndex) {
        if (availableSpace.get() <= 0) {
            int fullCapacity = cache.size();
            Set<Map.Entry<K, Cell<V>>> willBeRemove = cache
                    .entrySet().stream()
                    .limit(fullCapacity / 3)
                    .collect(Collectors.toSet());

            willBeRemove.forEach((entry) -> {
                cache.remove(entry.getKey());
                availableSpace.incrementAndGet();
            });
        }
        final Cell<V> newCell = new Cell<>();
        cache.put(cellIndex, newCell);
        availableSpace.decrementAndGet();
    }

    public long getSize() {
        return availableSpace.get();
    }

    public synchronized void clear() {
        availableSpace.addAndGet(cache.size());
        cache.clear();
    }

    public Class<K> getKeyType() {
        return keyType;
    }

    public Class<V> getValueType() {
        return valueType;
    }
}
