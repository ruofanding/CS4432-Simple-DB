package simpledb.buffer;

public enum Policy {
	leastRecentUsed,
	clock;
	
	public static Policy fromString(String str){
		if(str.equals("leastRecentUsed")){
			return leastRecentUsed;
		}else{
			return clock;
		}
	}
}
