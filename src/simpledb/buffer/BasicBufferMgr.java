package simpledb.buffer;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.PriorityQueue;

import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * * Manages the pinning and unpinning of buffers to blocks. * * @author Edward
 * Sciore *
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private LinkedList<Buffer> emptyBufferList;
	private Hashtable<Block, Buffer> blockToBuffer;
	private int numAvailable;

	private Policy policy;

	private PriorityQueue<Buffer> leastRecUsed;

	private int[] refBits;
	private int currentClockIndex;

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
		bufferpool = new Buffer[numbuffs];
		emptyBufferList = new LinkedList<Buffer>();
		numAvailable = numbuffs;
		blockToBuffer = new Hashtable<Block, Buffer>();
		this.policy = policy;

		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer(i);
			emptyBufferList.add(bufferpool[i]);
			bufferpool[i].updateTimeStamp();
		}

		switch (policy) {
		case clock:
			refBits = new int[numbuffs];
			for (int i = 0; i < numbuffs; i++) {
				refBits[i] = 1;
			}
			currentClockIndex = numbuffs - 1;
			break;
		case leastRecentUsed:
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
				bufferpool[i].updateTimeStamp();
				leastRecUsed.add(bufferpool[i]);
			}
			break;
		}

	}
	
	public String toString(){
		StringBuilder result = new StringBuilder();
		
		result.append("Basic buffer Manager\nPolicy:" + this.policy + "\nBufferpool contents:\n");
		for(Buffer buff : bufferpool){
			result.append(buff.toString() + "\n");
		}
		
		return result.toString();
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

		if (policy == Policy.leastRecentUsed) {
			leastRecUsed.remove(buff);
		}

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

		if (this.policy == Policy.leastRecentUsed) {
			leastRecUsed.remove(buff);
		}
		return buff;
	}

	/**
	 * * Unpins the specified buffer. * * @param buff * the buffer to be
	 * unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned()) {
			numAvailable++;
		}

		if (policy == Policy.leastRecentUsed) {
			buff.updateTimeStamp();
			leastRecUsed.add(buff);
		}
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
			return blockToBuffer.get(blk);
		} else {
			return null;
		}
	}

	private Buffer chooseUnpinnedBuffer() {
		Buffer buff;

		// Find empty buffer
		buff = findEmptyBuffer();

		// Use policy to find unpinned buffer if no empty buffer available.
		if (buff == null) {
			switch (this.policy) {
			case leastRecentUsed:
				buff = leastRecUsed.poll();
				break;
			case clock:
				for (int i = 0; i < bufferpool.length * 2; i++) {
					if (bufferpool[currentClockIndex].isPinned()) {
						currentClockIndex = (currentClockIndex + 1) % bufferpool.length;
						continue;
					} else if (refBits[currentClockIndex] == 1) {
						refBits[currentClockIndex] = 0;
						currentClockIndex = (currentClockIndex + 1) % bufferpool.length;
					} else {
						refBits[currentClockIndex] = 1;
						buff = bufferpool[currentClockIndex];
						break;
					}
				}
				break;
			}
		}

		if (buff != null) {
			if (buff.block() != null) {
				blockToBuffer.remove(buff.block());
			}
		}
		return buff;
	}

	private Buffer findEmptyBuffer() {
		return emptyBufferList.poll();
	}
}