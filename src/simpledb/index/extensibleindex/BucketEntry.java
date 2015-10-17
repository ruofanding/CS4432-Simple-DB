package simpledb.index.extensibleindex;

import java.util.ArrayList;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;


/** CS4432-Project2:
 * 
 * BucketEntry objects contain the positions of Buckets.
 * Used to find the correct Bucket to insert values to.
 */
public class BucketEntry {
	String idxname;
	Transaction tx;

	public BucketEntry(String idxname, Transaction tx){
		this.idxname = idxname;
		this.tx = tx;
	}

	/** CS4432-Project2:
	 *  Returns a string containing the positions 
	 */
	public String toString(){
		StringBuilder sb = new StringBuilder();
		TableScan ts = new TableScan(tableInfo(), tx);
		
		while(ts.next()){
			sb.append("pos=" + ts.getInt("pos") + "\n");
		}
		ts.close();
		return sb.toString();
	}
	
	/**
	 *  CS4432-Project2:
	 * @param index
	 * @return the position of bucket
	 */
	public int getBucketPos(int index){
		TableScan ts = new TableScan(tableInfo(), tx);
		
		for(int i = 0; i <= index; i++){
			if(!(ts.next())){
				ts.close();
				return -1;
			}
		}
			
		int pos = ts.getInt("pos");
		ts.close();
		return pos;
	}
	
	/** CS4432-Project2:
	 * Expands the bucket entry object to accommodate new entries
	 */
	public void expandEntry(){
		TableScan ts;
		
		ts= new TableScan(tableInfo(), tx);
		List<Integer> posList = new ArrayList<Integer>();
		while(ts.next()){
			posList.add(ts.getInt("pos"));
		}
		ts.close();

		ts = new TableScan(tableInfo(), tx);
		for(Integer pos: posList){
			ts.insert();
			ts.setInt("pos", pos);
		}
		ts.close();
	}

	/** CS4432-Project2:
	 *  Updates bucket entry for given bucket
	 * @param bucket
	 */
	public void updateEntry(Bucket bucket){
		TableScan ts;
		ts = new TableScan(tableInfo(), tx);

		int mask = ~((-1) << (bucket.depth)); 
		int index = 0;
		while(ts.next()){
			if((index & mask) == bucket.key){
				ts.setInt("pos", bucket.pos);
			}
			index++;
		}
		ts.close();
	}
	
	private TableInfo tableInfo() {
		Schema schema = new Schema();
		schema.addIntField("pos");
		return new TableInfo("ExtensibleIndex_" + idxname + "_bucketEntry", schema);
	}
	
	private static TableInfo tableInfo(String idxname){
		Schema schema = new Schema();
		schema.addIntField("pos");
		return new TableInfo("ExtensibleIndex_" + idxname + "_bucketEntry", schema);
	}

	public static void init(String idxname, Transaction tx){
		TableScan ts = new TableScan(tableInfo(idxname), tx);
		ts.insert();
		ts.setInt("pos", 0);
		ts.close();
	}
}
