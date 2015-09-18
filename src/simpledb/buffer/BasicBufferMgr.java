package simpledb.buffer;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.PriorityQueue;
import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * * Manages the pinning and unpinning of buffers to blocks. * * @author Edward
 * Sciore *
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int[] refBits;
	private int currentClockIndex;
	private HashSet<Buffer> emptyBufferSet;
	private Hashtable<Block, Buffer> blockToBuffer;
	private int numAvailable;
	private PriorityQueue<Buffer> leastRecUsed;
	private Policy policy;

	/**
	 * * Creates a buffer manager having the specified number of buffer slots. *
	 * This constructor depends on both the {@link FileMgr} and *
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class *
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * * initialization. Thus this constructor cannot be called until *
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called *
	 * first. * * @param numbuffs * the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs, Policy policy) {
		this.policy = policy;
		bufferpool = new Buffer[numbuffs];
		refBits = new int[numbuffs];
		emptyBufferSet = new HashSet<Buffer>();
		numAvailable = numbuffs;
		blockToBuffer = new Hashtable<Block, Buffer>();
		Comparator<Buffer> bufferComparator = new Comparator<Buffer>() {
			public int compare(Buffer o1, Buffer o2) {
				if (o1.timeStamp() == o2.timeStamp()) {
					return 0;
				} else if (o1.timeStamp() > o2.timeStamp()) {
					return 1;
				} else {
					return -1;
				}
			}
		};
		leastRecUsed = new PriorityQueue<Buffer>(bufferComparator);
		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer();
			emptyBufferSet.add(bufferpool[i]);
			bufferpool[i].updateTimeStamp();
			leastRecUsed.add(bufferpool[i]);
			refBits[i] = 0;
		}
		currentClockIndex = 0;
	}

	/**
	 * * Flushes the dirty buffers modified by the specified transaction. * * @param
	 * txnum * the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * * Pins a buffer to the specified block. If there is already a buffer *
	 * assigned to that block then that buffer is used; otherwise, an unpinned *
	 * buffer from the pool is chosen. Returns a null value if there are no *
	 * available buffers. * * @param blk * a reference to a disk block * @return
	 * the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		blockToBuffer.put(blk, buff);

		
		leastRecUsed.remove(buff);
		return buff;
	}

	/**
	 * * Allocates a new block in the specified file, and pins a buffer to it. *
	 * Returns null (without allocating the block) if there are no available *
	 * buffers. * * @param filename * the name of the file * @param fmtr * a
	 * pageformatter object, used to format the new block * @return the pinned
	 * buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		buff.pin();
		blockToBuffer.put(buff.block(), buff);
		
		leastRecUsed.remove(buff);
		return buff;
	}

	/**
	 * * Unpins the specified buffer. * * @param buff * the buffer to be
	 * unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned()) {
			emptyBufferSet.add(buff);
			numAvailable++;
		}
		
		buff.updateTimeStamp();
		leastRecUsed.add(buff);
	}

	/**
	 * * Returns the number of available (i.e. unpinned) buffers. * * @return
	 * the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		if (blockToBuffer.containsKey(blk)) {
			leastRecUsed.poll();
			return blockToBuffer.get(blk);
		} else {
			return null;
		} /*
		 * * for (Buffer buff : bufferpool) { Block b = buff.block(); if (b != *
		 * null && b.equals(blk)) return buff; } return null;
		 */
	}

	private Buffer chooseUnpinnedBuffer() {
		Buffer buff;
		buff = findEmptyBuffer();
		
		if(buff == null){
			if (policy == Policy.leastRecentUsed){
				buff = leastRecUsed.poll();
			} else {
				for(int i = 0; i < bufferpool.length * 2; i++){
					if(bufferpool[currentClockIndex].isPinned()){
						continue;
					} else if(refBits[currentClockIndex] == 1){
						refBits[currentClockIndex] = 0;
						currentClockIndex = (currentClockIndex + 1) % bufferpool.length;
					} else {
						refBits[currentClockIndex] = 1;
						buff = bufferpool[currentClockIndex];
						break;
					}
				}
			}
		}
		return buff;
	}

	private Buffer findEmptyBuffer() {
		Buffer buff;
		if (!emptyBufferSet.isEmpty()) {
			buff = emptyBufferSet.iterator().next();
			emptyBufferSet.remove(buff);
			return buff;
		} else {
			return null;
		}
	}
}