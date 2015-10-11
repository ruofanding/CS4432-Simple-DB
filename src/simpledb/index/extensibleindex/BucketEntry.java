package simpledb.index.extensibleindex;

import simpledb.query.Constant;
import simpledb.record.RID;

public class BucketEntry {
	Constant dataval;
	RID datarid;
	
	public BucketEntry(Constant dataval, RID datarid){
		this.datarid = datarid;
		this.dataval = dataval;
	}
}
