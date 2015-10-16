package simpledb.index.extensibleindex;

import java.util.Iterator;
import java.util.List;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

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
/*
	private TableScan moveToIndex(int index) {
		TableInfo ti = getEHTableInfo(idxname);
		TableScan ts = new TableScan(ti, tx);
		ts.beforeFirst();

		for (int i = 0; i <= index + 1; i++) {
			ts.next();
		}
		return ts;
	}

	private Bucket getBucketFromTS(TableScan ts) {
		return new Bucket(ts.getInt("bucketId"), ts.getInt("depth"),
				ts.getInt("size"), idxname, sch, tx);
	}

	private void updateTSWithBucket(Bucket bucket, TableScan ts) {
		if (ts.getInt("depth") != bucket.depth)
			ts.setInt("depth", bucket.depth);
		if (ts.getInt("size") != bucket.size)
			ts.setInt("size", bucket.size);
		if (ts.getInt("bucketId") != bucket.id)
			ts.setInt("bucketId", bucket.id);
	}
	
	private void updateRelevantBucket(Bucket bucket){
		TableScan ts = getTableScan();
		ts.next();
		int index = 0;
		int mask = ~((-1) << (bucket.depth)); 
		while(ts.next()){
			if((index & mask) == bucket.id){
				updateTSWithBucket(bucket, ts);
			}
			index++;
		}
		ts.close();
	}

	private Bucket getBucketWithIndex(int index) {
		TableScan ts = moveToIndex(index);
		Bucket bucket = getBucketFromTS(ts);
		ts.close();
		return bucket;
	}

	private List<Bucket> getAllBucket() {
		TableInfo ti = getEHTableInfo(idxname);
		TableScan ts = new TableScan(ti, tx);
		List<Bucket> list = new ArrayList<Bucket>();
		ts.beforeFirst();

		if (!ts.next()) {
			return null;
		}
		while (ts.next()) {
			list.add(getBucketFromTS(ts));
		}
		ts.close();

		return list;
	}

	private void insertBucketList(List<Bucket> list) {
		TableInfo ti = getEHTableInfo(idxname);
		TableScan ts = new TableScan(ti, tx);
		ts.beforeFirst();

		for (Bucket bucket : list) {
			ts.insert();
			ts.setInt("depth", bucket.depth);
			ts.setInt("size", bucket.size);
			ts.setInt("bucketId", bucket.id);
		}

		ts.close();
	}
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
		/*
		Bucket currentBucket;
		currentBucket = getBucketWithIndex(dataval.hashCode() % size);
		currentBucket.delete(dataval, datarid);*/
	}

	@Override
	public void close() {
		currentIterator = null;
	}

}
