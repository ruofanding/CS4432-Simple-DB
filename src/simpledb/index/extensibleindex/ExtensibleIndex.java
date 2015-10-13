package simpledb.index.extensibleindex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class ExtensibleIndex implements Index{
	static class Cache{
		String idxname;
		int depth;
		int size;
		ArrayList<Bucket> bucketList;
			

		public Cache(String idxname, int depth, int size, ArrayList<Bucket> bucketList){
			this.idxname = idxname;
			this.depth = size;
			this.size = size;
			this.bucketList = bucketList;
		}

		public void update(int depth, int size){
			this.size = size;
			this.depth = depth;
		}
	}

	static Cache cache= null;
	final int INIT_SIZE = 1;
	String idxname;
	Schema sch;
	Transaction tx;
	
	ArrayList<Bucket> bucketList;
	Iterator<RID> currentIterator = null;
	int depth;
	int size;
	
	public static TableInfo getEHTableInfo(String idxname){
		Schema indexSchema = new Schema();
		indexSchema.addIntField("depth");
		indexSchema.addIntField("bucketId");
		indexSchema.addIntField("size");
		indexSchema.addIntField("index");
		return new TableInfo("ExtensibleIndex" + idxname, indexSchema);
	}
	
	public ExtensibleIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
		
		if(cache != null && cache.idxname == idxname){
			this.size = cache.size;
			this.depth = cache.depth;
			this.bucketList = cache.bucketList;
			return;
		}

		TableInfo ti = getEHTableInfo(idxname);
		TableScan ts = new TableScan(ti, tx);
		
		if(ts.next()){
			if(ts.getInt("index") == -1){
				depth = ts.getInt("depth");
				size = ts.getInt("size");
			} else {
				System.err.println("The first record is not for global");
			}
		} else {
			System.err.println("No extensible index created");
		}
		
				bucketList = new ArrayList<Bucket>();
		while(ts.next()){
			Bucket newBucket= new Bucket(ts.getInt("bucketId"), ts.getInt("depth"), ts.getInt("size"), idxname, sch, tx);
			bucketList.add(newBucket);
		}
		ts.close();

		cache = new Cache(idxname, depth, size, bucketList);
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();
        for(int i = 0; i < bucketList.size(); i++){
			sb.append("[" + i + "]:" + bucketList.get(i).toString());
			sb.append("\n");
		}
        return sb.toString();
	}
	
	public void beforeFirst(Constant searchkey) {
		Bucket currentBucket = bucketList.get(searchkey.hashCode() % size);
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

	@Override
	public void insert(Constant dataval, RID datarid) {
		Bucket currentBucket;
		currentBucket = bucketList.get(dataval.hashCode() % size);
		while(currentBucket.isFull()){
			if(currentBucket.depth == this.depth){
				for(int i = 0; i < size ; i++){
					bucketList.add(bucketList.get(i));
				}
				size = size * 2;
				this.depth++;
			}
			
			List<BucketEntry> bucketEntries = currentBucket.redistribute();
			Bucket newBucket = new Bucket(currentBucket.id + (1<< (currentBucket.depth - 1)), currentBucket.depth, 0, idxname, sch, tx);
			for(BucketEntry entry : bucketEntries){
				newBucket.insert(entry.dataval, entry.datarid);
			}
			bucketList.set(newBucket.id, newBucket);
			
			int index = dataval.hashCode() % size;
			currentBucket = bucketList.get(index);
		}
		
		currentBucket.insert(dataval, datarid);
		TableInfo ti = getEHTableInfo(idxname);
		TableScan ts = new TableScan(ti, tx);
		ts.beforeFirst();
		
		if(ts.next() && ts.getInt("index") == -1){
			if(ts.getInt("size") != this.size) ts.setInt("size", this.size);
			if(ts.getInt("depth") != this.depth) ts.setInt("depth", this.depth);
		} else {
			System.err.println("Global record does not exist");
		}
		

		for(int i = 0; i < bucketList.size(); i++){
			Bucket b = bucketList.get(i);
			if(ts.next()){
				if(ts.getInt("index") == i){
					if(ts.getInt("depth") != b.depth) ts.setInt("depth", b.depth);
					if(ts.getInt("size") != b.size) ts.setInt("size", b.size);
					if(ts.getInt("bucketId") != b.id) ts.setInt("bucketId", b.id);
				}else{
					System.err.println("Index doesn't match");
				}
			} else {
				ts.insert();
				ts.setInt("index", i);
				ts.setInt("depth", b.depth);
				ts.setInt("size", b.size);
				ts.setInt("bucketId", b.id);
			}
		}
		ts.close();
		cache.update(depth, size);
	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		Bucket currentBucket;
		currentBucket = bucketList.get(dataval.hashCode() % size);
		currentBucket.delete(dataval, datarid);
	}

	@Override
	public void close() {
		currentIterator = null;
	}

}
