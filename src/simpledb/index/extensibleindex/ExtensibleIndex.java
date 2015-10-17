package simpledb.index.extensibleindex;

import java.util.Iterator;
import java.util.List;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/** CS4432-Project2:
 * 
 * Extensible hash index implementation
 *
 */
public class ExtensibleIndex implements Index {
	String idxname;
	Schema sch;
	Transaction tx;

	Iterator<RID> currentIterator = null;
	int depth;
	int size;
	int bucketNum;

	EhGlobal globalManager;
	BucketEntry bucketEntryManager;
	BucketList bucketListManager;

	public ExtensibleIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;

		globalManager = new EhGlobal(idxname, sch, tx);
		this.depth = globalManager.getDepth();
		this.size = 1 << this.depth;
		bucketNum = globalManager.getBucketNum();

		bucketEntryManager = new BucketEntry(idxname, tx);
		bucketListManager = new BucketList(idxname, sch, tx);
	}
	
	public static void init(String idxname, Transaction tx){
		EhGlobal.init(idxname, tx);
		BucketEntry.init(idxname, tx);
		BucketList.init(idxname, tx);
	}
	
	public void debug() {
		System.out.print(globalManager.toString());
		System.out.print(bucketEntryManager.toString());
		System.out.println(bucketListManager.toString());
	}

	public void beforeFirst(Constant searchkey) {
		int index = searchkey.hashCode() % size;
		int pos = bucketEntryManager.getBucketPos(index);
		Bucket currentBucket = bucketListManager.getBucket(pos);
		currentIterator = currentBucket.getDataRid(searchkey).iterator();
	}

	@Override
	public boolean next() {
		return currentIterator.hasNext();
	}

	@Override
	public RID getDataRid() {
		return currentIterator.next();
	}

	/** CS4432-Project2:
	 * Used to insert new values
	 */
	@Override
	public void insert(Constant dataval, RID datarid) {
		int index = dataval.hashCode() % this.size;
		int pos = this.bucketEntryManager.getBucketPos(index);
		
		Bucket currentBucket = bucketListManager.getBucket(pos);
		while (currentBucket.isFull()) {
			if (currentBucket.depth == this.depth) {
				bucketEntryManager.expandEntry();
				this.size = size * 2;
				this.depth++;
				globalManager.updateDepth(depth);
			}

			List<DataEntry> bucketEntries = currentBucket.redistribute();
			globalManager.updateBucketNum(++bucketNum);
			Bucket newBucket = new Bucket(currentBucket.key
					+ (1 << (currentBucket.depth - 1)), currentBucket.depth, 0, bucketNum-1, 
					idxname, sch, tx);
			for (DataEntry entry : bucketEntries) {
				newBucket.insert(entry.dataval, entry.datarid);
			}

			bucketEntryManager.updateEntry(currentBucket);
			bucketListManager.updateBucket(currentBucket);

			bucketListManager.insertNewBucket(newBucket);
			bucketEntryManager.updateEntry(newBucket);

			index = dataval.hashCode() % size;
			pos = bucketEntryManager.getBucketPos(index);
			currentBucket = bucketListManager.getBucket(pos);
		}

		currentBucket.insert(dataval, datarid);
		bucketListManager.updateBucket(currentBucket);
	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		int index = dataval.hashCode() % size;
		int pos = bucketEntryManager.getBucketPos(index);
		Bucket currentBucket = bucketListManager.getBucket(pos);
		
		currentBucket.delete(dataval, datarid);
		bucketListManager.updateBucket(currentBucket);
	}

	@Override
	public void close() {
		currentIterator = null;
	}

}
