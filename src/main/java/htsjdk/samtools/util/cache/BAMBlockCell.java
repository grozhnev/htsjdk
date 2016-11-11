package htsjdk.samtools.util.cache;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.BlockCompressedInputStream;

import java.util.List;

public class BAMBlockCell extends AbstractCell<BlockCompressedInputStream.Block, BAMBlockCell.BAMBlockSlot> {

    BAMBlockCell(int size) {
        super(size);
    }

    @Override
    protected BAMBlockSlot initSlot() {
        return new BAMBlockSlot();
    }

    public class BAMBlockSlot {

        public void putBlock(BlockCompressedInputStream.Block block) {
            try {
                dataReady.put(block);
                increaseProgress(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Entry {
        public final List<SAMRecord> records;
        public final long bytesLen;

        public Entry(List<SAMRecord> records, long bytesLen) {
            this.records = records;
            this.bytesLen = bytesLen;
        }
    }

}
