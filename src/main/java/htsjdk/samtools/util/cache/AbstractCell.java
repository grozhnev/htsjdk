package htsjdk.samtools.util.cache;

import htsjdk.samtools.util.RuntimeIOException;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

abstract class AbstractCell <D, S> {

    private static final int SINGLE_SLOT_CAPACITY = 1;

    private final Queue<S> forFilling;
    final BlockingQueue<D> dataReady;

    private volatile int progress;
    private S slot;
    private final int size;

    AbstractCell(int size) {
        this.size = size;
        forFilling = new ArrayBlockingQueue<>(SINGLE_SLOT_CAPACITY);
        dataReady = new ArrayBlockingQueue<>(SINGLE_SLOT_CAPACITY);

        slot = initSlot();
        forFilling.add(slot);
    }

    public D data() {
        try {
            final D take = dataReady.take();
            dataReady.put(take);
            return take;
        } catch (InterruptedException e) {
            throw new RuntimeIOException(e);
        }
    }

    public synchronized Optional<S> takeSlot() {
        return Optional.ofNullable(forFilling.poll());
    }

    public synchronized boolean freeSlot(S slot) {
        if (slot != this.slot) {
            throw new RuntimeException("The slot is not correspond to this cell");
        }
        return forFilling.add(this.slot);
    }

    public synchronized boolean isInProgress() {
        return forFilling.isEmpty();
    }

    public int progress() {
        return progress;
    }

    public boolean isFull() {
        return progress == size;
    }

    public int getSize() {
        return size;
    }

    protected void increaseProgress(int delta) {
        progress += delta;
    }

    protected abstract S initSlot();
}
