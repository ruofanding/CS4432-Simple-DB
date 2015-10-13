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
	final int INIT_SIZE = 1;
	String idxname;
	Schema sch;
	Transaction tx;
	
	ArrayList<Bucket> bucketList;
	Bucket currentBucket = null;
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
		
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
		
		bucketList = new ArrayList<Bucket>();
		while(ts.next()){
			Bucket newBucket= new Bucket(ts.getInt("bucketId"), ts.getInt("depth"), ts.getInt("size"), idxname, sch, tx);
			bucketList.add(newBucket);
		}
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
		close();
		int index = searchkey.hashCode() % (1<<depth);
		currentBucket = bucketList.get(index);
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
			ts.setInt("size", this.size);
			ts.setInt("depth", this.depth);
		} else {
			System.err.println("Global record does not exist");
		}
		

		for(int i = 0; i < bucketList.size(); i++){
			Bucket b = bucketList.get(i);
			if(ts.next()){
				if(ts.getInt("index") == i){
					ts.setInt("depth", b.depth);
					ts.setInt("size", b.size);
					ts.setInt("bucketId", b.id);
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
	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		currentBucket = bucketList.get(dataval.hashCode() % size);
		currentBucket.delete(dataval, datarid);
	}

	@Override
	public void close() {
		if(currentBucket != null){
			currentBucket = null;
		}
		currentIterator = null;
	}

}
