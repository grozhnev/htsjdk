package htsjdk.samtools.util.cache;

import htsjdk.samtools.util.RuntimeIOException;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @see Cache
 * @param <V> - the type of the values held within this cache.
 */
public class Cell<V> {

    private static final int SINGLE_SLOT_CAPACITY = 1;

    private final Queue<Slot> forFilling;
    private final BlockingQueue<V> dataReady;

    private Slot slot;

    Cell() {
        forFilling = new ArrayBlockingQueue<>(SINGLE_SLOT_CAPACITY);
        dataReady = new ArrayBlockingQueue<>(SINGLE_SLOT_CAPACITY);

        slot = new Slot();
        forFilling.add(slot);
    }

    public V getValue() {
        try {
            final V take = dataReady.take();
            dataReady.put(take);
            return take;
        } catch (InterruptedException e) {
            throw new RuntimeIOException(e);
        }
    }

    /**
     * If a thread wants to write a value to the Cell, it can try to take a Slot to has write access to the Cell.
     * There is only one slot each Cell has, so if no slot returns from the method it means slot was taken.
     * Absence of the Slot means Cell is under filling (putting value to it by another thread) or it is already filled.
     * Slot can be returned to the Cell by {@link #freeSlot(Slot)} method for example if there was an Error during
     * cell filling.
     * @see #isFull()
     * @see #freeSlot(Slot)
     * @return {@link Optional} of a current cell slot or Optional of null if slot was already taken.
     */
    public synchronized Optional<Slot> takeSlot() {
        return Optional.ofNullable(forFilling.poll());
    }

    /**
     * @see #takeSlot()
     * @param slot - slot to be returned.
     * @return {@code true} if slot was returned successfully.
     */
    public synchronized boolean freeSlot(Slot slot) {
        if (slot != this.slot) {
            throw new RuntimeException("The slot is not correspond to this cell");
        }
        return forFilling.add(this.slot);
    }

    public synchronized boolean isInProgress() {
        return forFilling.isEmpty();
    }

    public boolean isFull() {
        return !dataReady.isEmpty();
    }

    /**
     * Represents an abstraction of write access to the {@link Cell}.
     * @see Cell
     * @see Cache
     */
    public class Slot {
        public void store(V entry) {
            try {
                dataReady.put(entry);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
