/*
 * The MIT License
 *
 * Copyright (c) 2016 Daniel Cameron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools.util;

import htsjdk.samtools.Defaults;
import htsjdk.samtools.seekablestream.SeekableStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

/**
 * Asynchronous read-ahead implementation of
 * {@link htsjdk.samtools.util.BlockCompressedInputStream}.
 * 
 * Note that this implementation is not synchronized. If multiple threads access
 * an instance concurrently, it must be synchronized externally.
 */
public class AsyncBlockCompressedInputStream
		extends BlockCompressedInputStream {
	private static final int READ_AHEAD_BUFFERS = (int) Math
			.ceil(Defaults.NON_ZERO_BUFFER_SIZE
					/ BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE);
	private static final Executor threadpool = Executors.newFixedThreadPool(1,
			new ThreadFactory() {
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setDaemon(true);
					return t;
				}
			});
	
	private final BlockingQueue<byte[]> freeBuffers = new ArrayBlockingQueue<>(
			READ_AHEAD_BUFFERS);
	/**
	 * Indicates whether a read-ahead task has been scheduled to run. Only one
	 * read-ahead task per stream can be scheduled at any one time.
	 */
	private final Semaphore running = new Semaphore(READ_AHEAD_BUFFERS);
	private CompletableFuture<DecompressedBlock> lastCall;
	/**
	 * Indicates whether any scheduled task should abort processing and
	 * terminate as soon as possible since the result will be discarded anyway.
	 */

	public AsyncBlockCompressedInputStream(final InputStream stream) {
		super(stream, true);
	}

	public AsyncBlockCompressedInputStream(final File file) throws IOException {
		super(file);
	}

	public AsyncBlockCompressedInputStream(final URL url) {
		super(url);
	}

	public AsyncBlockCompressedInputStream(final SeekableStream strm) {
		super(strm);
	}

	@Override
	protected DecompressedBlock nextBlock(byte[] bufferAvailableForReuse) {
		if (bufferAvailableForReuse != null) {
			freeBuffers.offer(bufferAvailableForReuse);
		}
		return nextBlockSync();
	}

	private DecompressedBlock nextBlockSync() {
		
		if (lastCall == null) {
			lastCall = new CompletableFuture<DecompressedBlock>();
		}
		
		CompletableFuture<DecompressedBlock> prevCall = lastCall;
		
		lastCall = lastCall.thenSupplyAsync(freeBuffers::poll, threadpool)
				.thenApplyAsync(this::processNextBlock, threadpool);
		
		return prevCall.join();

	}

}