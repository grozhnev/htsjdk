package htsjdk.samtools.util.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class AbstractCache<C extends AbstractCell> {

    protected AtomicLong availableSpace;
    protected final Map<Long, C> cache;
    private final long initialSize;

    protected AbstractCache(AtomicLong availableSpace) {
        this.availableSpace= availableSpace;
        initialSize = availableSpace.get();
        cache = new LinkedHashMap<>();
    }

    public int getCurrentSize() {
        return cache.size();
    }

    public synchronized C observeCell(long cellIndex) {
        if (!cache.containsKey(cellIndex)) {
            allocCell(cellIndex);
        }

        C cell = cache.remove(cellIndex);
        cache.put(cellIndex, cell);

        return cell;
    }

    private void allocCell(long cellIndex) {
        if (availableSpace.get() <= 0) {
            int fullCapacity = cache.size();
            Set<Map.Entry<Long, C>> willBeRemove = cache
                    .entrySet().stream()
                    .limit(fullCapacity / 3)
                    .collect(Collectors.toSet());

            willBeRemove.forEach((entry) -> {
                cache.remove(entry.getKey());
                availableSpace.addAndGet(entry.getValue().getSize());
            });
        }
        final C newCell = newCell(cellIndex);
        cache.put(cellIndex, newCell);
        availableSpace.addAndGet(-newCell.getSize());
    }

    protected abstract C newCell(long index);

    public long getSize() {
        return availableSpace.get();
    }

    public synchronized void clear() {
        cache.clear();
        availableSpace.set(initialSize);
    }
}
