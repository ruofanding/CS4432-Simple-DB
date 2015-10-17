package simpledb.opt;

import simpledb.metadata.TableMgr;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class SortedTableManager {
	private static SortedTableManager instance = null;
	
	public static SortedTableManager getManager(){
		if(instance == null){
			instance = new SortedTableManager();
		}
		return instance;
	}

	public String toString(){
		Transaction tx = new Transaction();
		StringBuilder sb = new StringBuilder();
		TableScan ts = new TableScan(tableInfo(), tx);
		
		sb.append("Global:");
		while(ts.next()){
			sb.append("\tdepth=" + ts.getInt("depth") + "\n");
			sb.append("\tbucketNum=" + ts.getInt("bucketnum") + "\n");
		}
		ts.close();
		tx.commit();
		return sb.toString();
	}
		
	public boolean isSorted(String tblname, String column){
		Transaction tx = new Transaction();
		TableScan ts = new TableScan(tableInfo(), tx);

		boolean result = false;
		while(ts.next()){
			if(ts.getString("tblname").equals(tblname) && ts.getString("column").equals(column)){
				result = ts.getInt("sorted") == 1;
				break;
			}
		}
		ts.close();
		tx.commit();

		return result;
	}

	public void setSorted(String tblname, String column){
		Transaction tx = new Transaction();
		TableScan ts = new TableScan(tableInfo(), tx);
		boolean set = false;

		while(ts.next()){
			if(ts.getString("tblname").equals(tblname)) {
				ts.setString("column", column);
				ts.setInt("sorted", 1);
				set = true;
			}
		}
		if(!set){
			ts.insert();
			ts.setString("tblname", tblname);
			ts.setString("column", column);
			ts.setInt("sorted", 1);
		}
		ts.close();
		tx.commit();
	}

	private TableInfo tableInfo() {
		Schema schema = new Schema();
		schema.addStringField("tblname", TableMgr.MAX_NAME);
		schema.addIntField("sorted");
		schema.addStringField("column", TableMgr.MAX_NAME);
		return new TableInfo("SortedTableMgr", schema);
	}

}
