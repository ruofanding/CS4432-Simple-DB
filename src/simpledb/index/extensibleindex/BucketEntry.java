package simpledb.index.extensibleindex;

import java.util.ArrayList;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class BucketEntry {
	String idxname;
	Transaction tx;

	public BucketEntry(String idxname, Transaction tx){
		this.idxname = idxname;
		this.tx = tx;
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();
		TableScan ts = new TableScan(tableInfo(), tx);
		
		while(ts.next()){
			sb.append("pos=" + ts.getInt("pos") + "\n");
		}
		ts.close();
		return sb.toString();
	}
	
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
