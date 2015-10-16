package simpledb.index.extensibleindex;

import simpledb.query.Constant;
import simpledb.record.RID;

public class DataEntry {
	Constant dataval;
	RID datarid;
	
	public DataEntry(Constant dataval, RID datarid){
		this.datarid = datarid;
		this.dataval = dataval;
	}
}
