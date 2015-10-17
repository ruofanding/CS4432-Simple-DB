package simpledb.index.extensibleindex;

import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * CS4432-Project2:
 * Metadata containing index name, transaction and schema
 * to be used with extensible hash index
 *
 */
public class EhGlobal {
	String idxname;
	Transaction tx;
	Schema sch;

	
	public EhGlobal(String idxname, Schema sch, Transaction tx){
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();
		TableScan ts = new TableScan(tableInfo(), tx);
		
		sb.append("Global:");
		while(ts.next()){
			sb.append("\tdepth=" + ts.getInt("depth") + "\n");
			sb.append("\tbucketNum=" + ts.getInt("bucketnum") + "\n");
		}
		ts.close();
		return sb.toString();
	}
	
	public static void init(String idxname, Transaction tx){
		TableScan ts = new TableScan(tableInfo(idxname), tx);
		ts.insert();
		ts.setInt("depth", 0);
		ts.setInt("bucketnum", 1);
		ts.close();
	}
	
	public void updateDepth(int depth){
		TableScan ts = new TableScan(tableInfo(), tx);
		ts.next();
		ts.setInt("depth", depth);
		ts.close();
	}
	
	public void updateBucketNum(int num){
		TableScan ts = new TableScan(tableInfo(), tx);
		ts.next();
		ts.setInt("bucketnum", num);
		ts.close();
	}

	public int getBucketNum(){
		TableScan ts = new TableScan(tableInfo(), tx);
		ts.next();
		int depth = ts.getInt("bucketnum");
		ts.close();
		return depth;
	}

	public int getDepth(){
		TableScan ts = new TableScan(tableInfo(), tx);
		ts.next();
		int depth = ts.getInt("depth");
		ts.close();
		return depth;
	}
	
	private TableInfo tableInfo() {
		Schema schema = new Schema();
		schema.addIntField("depth");
		schema.addIntField("bucketnum");
		return new TableInfo("ExtensibleIndex_" + idxname + "_global", schema);
	}


	private static TableInfo tableInfo(String idxname){
		Schema schema = new Schema();
		schema.addIntField("depth");
		schema.addIntField("bucketnum");
		return new TableInfo("ExtensibleIndex_" + idxname + "_global", schema);
	}
}
