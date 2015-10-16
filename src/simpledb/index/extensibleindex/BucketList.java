package simpledb.index.extensibleindex;

import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class BucketList {
	
	String idxname;
	Transaction tx;
	Schema sch;

	
	public BucketList(String idxname, Schema sch, Transaction tx){
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
	}

	public static void init(String idxname, Transaction tx){
		TableScan ts = new TableScan(tableInfo(idxname), tx);
		ts.insert();
		ts.setInt("key", 0);
		ts.setInt("depth", 0);
		ts.setInt("size", 0);
		ts.close();
	}

	public void insertNewBucket(Bucket bucket){
		TableScan ts = new TableScan(tableInfo(), tx);
		
		ts.insert();
		ts.setInt("depth", bucket.depth);
		ts.setInt("size", bucket.size);
		ts.setInt("key", bucket.key);
		ts.close();
	}
	
	public void updateBucket(Bucket bucket){
		TableScan ts = new TableScan(tableInfo(), tx);
		
		for(int i = 0; i <= bucket.pos; i++){
			if(!ts.next()){
				System.err.println("Fail to update a bucket info");
				ts.close();
				return;
			}
		}
		
		if (ts.getInt("depth") != bucket.depth)
			ts.setInt("depth", bucket.depth);
		if (ts.getInt("size") != bucket.size)
			ts.setInt("size", bucket.size);
		ts.close();
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();
		TableScan ts = new TableScan(tableInfo(), tx);
		
		while(ts.next()){
			sb.append("key=" + ts.getInt("key") + ", size=" + ts.getInt("size") + ", depth=" + ts.getInt("depth") + "\n");
		}
		ts.close();
		return sb.toString();
	}	

	public Bucket getBucket(int pos){
		TableScan ts;
		ts = new TableScan(tableInfo(), tx);
		
		for(int i = 0; i <= pos; i++){
			if(!ts.next()){
				ts.close();
				return null;
			}
		}
		
		Bucket bucket = new Bucket(ts.getInt("key"), ts.getInt("depth"), ts.getInt("size"), pos, idxname, sch, tx);
		ts.close();
		return bucket;
	}

	private TableInfo tableInfo() {
		Schema schema = new Schema();
		schema.addIntField("depth");
		schema.addIntField("key");
		schema.addIntField("size");
		return new TableInfo("ExtensibleIndex_" + idxname + "_bucketList", schema);
	}

	private static TableInfo tableInfo(String idxname){
		Schema schema = new Schema();
		schema.addIntField("depth");
		schema.addIntField("key");
		schema.addIntField("size");
		return new TableInfo("ExtensibleIndex_" + idxname + "_bucketList", schema);
	}

}
