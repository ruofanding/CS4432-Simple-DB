package simpledb.opt;

import java.util.HashSet;

import simpledb.metadata.TableMgr;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;


/**
 * CS4432-Project2: A class check, and set the whether a table is updated.
 * @author ruofanding
 *
 */
public class SortedTableManager {
	private HashSet<String> unsorted;
	private static SortedTableManager instance = null;
	
	private SortedTableManager(){
		unsorted = new HashSet<String>();
	}

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
		if(unsorted.contains(tblname)){
			unsorted.remove(tblname);
		}

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

	public void setUnSorted(String tblname){
		if(unsorted.contains(tblname)){
			return;
		}

		Transaction tx = new Transaction();
		TableScan ts = new TableScan(tableInfo(), tx);

		while(ts.next()){
			if(ts.getString("tblname").equals(tblname)) {
				ts.setInt("sorted", 0);
			}
		}

		unsorted.add(tblname);
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
