package simpledb.index.extensibleindex;

import java.util.ArrayList;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class Bucket {
	final int BUCKET_SIZE = 10;
	int depth;
	int id;
	int size;
	private String tblname;
	private Schema sch;
	private Transaction tx;
	
	public Bucket(int id, int depth, int size, String tblname, Schema sch, Transaction tx){
		this.tblname = tblname;
		this.sch = sch;
		this.id = id;
		this.tx = tx;
		
		this.size = size;
		this.depth = depth;
	}
	
	public void insert(Constant dataval, RID datarid) {
		TableInfo ti = new TableInfo(tblname + id, sch);
		TableScan ts = new TableScan(ti, tx);
		ts.insert();
		ts.setInt("block", datarid.blockNumber());
		ts.setInt("id", datarid.id());
		ts.setVal("dataval", dataval);
		size++;
		ts.close();
	}
	
	public List<RID> getDataRid(Constant dataval){
		List<RID> result = new ArrayList<RID>();
		TableInfo ti = new TableInfo(tblname + id, sch);
		TableScan ts = new TableScan(ti, tx);
		
		while(ts.next()){
			int blknum = ts.getInt("block");
			int id = ts.getInt("id");
			Constant val = ts.getVal("dataval");
			RID rid = new RID(blknum, id);

			if(val.equals(dataval)){
				result.add(rid);
			}
		}
		ts.close();
		return result;
	}
	
	public void delete(Constant dataval, RID datarid) {
		TableInfo ti = new TableInfo(tblname + id, sch);
		TableScan ts = new TableScan(ti, tx);
		
		while(ts.next()){
			int blknum = ts.getInt("block");
			int id = ts.getInt("id");
			RID current = new RID(blknum, id);
			if(current.equals(datarid)){
				ts.delete();
				size--;
				ts.close();
				return;
			}
		}
		ts.close();
	}
	
	public boolean isFull(){
		return size >= BUCKET_SIZE;
	}
	
	public List<BucketEntry> redistribute(){
		TableInfo ti = new TableInfo(tblname + id, sch);
		TableScan ts = new TableScan(ti, tx);
		List<BucketEntry> result = new ArrayList<BucketEntry>();
		while(ts.next()){
			int blknum = ts.getInt("block");
			int id = ts.getInt("id");
			Constant val = ts.getVal("dataval");
			RID rid = new RID(blknum, id);
			
			int index = val.hashCode() % (1 << (depth + 1));
			if(index != this.id){
				ts.delete();
				result.add(new BucketEntry(val, rid));
			}
		}
		
		this.depth++;
		size = size - result.size();
		ts.close();
		return result;
	}

	public String toString(){
		return ("Id=" + this.id + ", size=" + size + ", depth" + depth);
	}
	
}
