package simpledb.index.extensibleindex;

import java.util.ArrayList;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/** CS4432-Project2:
 * Buckets are used to store entries when extensible hash index is used
 *
 */
public class Bucket {
	final int BUCKET_SIZE = 200; 
	int depth;
	int key;
	int pos;
	int size;
	private String tblname;
	private Schema sch;
	private Transaction tx;
	
	public Bucket(int id, int depth, int size, int pos, String tblname, Schema sch, Transaction tx){
		this.tblname = tblname;
		this.sch = sch;
		this.key = id;
		this.tx = tx;
		
		this.size = size;
		this.depth = depth;
		this.pos = pos;
	}
	
	/** CS4432-Project2:
	 * Insert a new value to the bucket
	 * @param dataval 
	 * @param datarid
	 */
	public void insert(Constant dataval, RID datarid) {
		TableInfo ti = new TableInfo(tblname + key, sch);
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
		TableInfo ti = new TableInfo(tblname + key, sch);
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
	
	/** CS4432-Project2:
	 * Delete a value from a bucket
	 * @param dataval
	 * @param datarid
	 */
	public void delete(Constant dataval, RID datarid) {
		TableInfo ti = new TableInfo(tblname + key, sch);
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
	
	/** CS4432-Project2:
	 * Redistribute entries in a bucket if bucket is full
	 * after expanding.
	 * @return
	 */
	public List<DataEntry> redistribute(){
		TableInfo ti = new TableInfo(tblname + key, sch);
		TableScan ts = new TableScan(ti, tx);
		List<DataEntry> result = new ArrayList<DataEntry>();
		while(ts.next()){
			int blknum = ts.getInt("block");
			int id = ts.getInt("id");
			Constant val = ts.getVal("dataval");
			RID rid = new RID(blknum, id);
			
			int index = val.hashCode() % (1 << (depth + 1));
			if(index != this.key){
				ts.delete();
				result.add(new DataEntry(val, rid));
			}
		}
		
		this.depth++;
		size = size - result.size();
		ts.close();
		return result;
	}

	public String toString(){
		return ("Id=" + this.key + ", size=" + size + ", depth" + depth);
	}
	
}
