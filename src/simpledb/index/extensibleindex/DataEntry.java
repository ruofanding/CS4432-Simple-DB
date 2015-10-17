package simpledb.index.extensibleindex;

import simpledb.query.Constant;
import simpledb.record.RID;
/** CS4432-Project2:
 * Data entry class for buckets
 *
 */
public class DataEntry {
	Constant dataval;
	RID datarid;
	
	public DataEntry(Constant dataval, RID datarid){
		this.datarid = datarid;
		this.dataval = dataval;
	}
}
