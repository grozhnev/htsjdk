package htsjdk.samtools.util.cache;

import java.util.concurrent.atomic.AtomicLong;

public class BAMCache extends AbstractCache<BAMBlockCell> {

    public BAMCache(AtomicLong availableSpace) {
        super(availableSpace);
    }

    @Override
    protected BAMBlockCell newCell(long index) {
        return new BAMBlockCell(1);
    }

}
