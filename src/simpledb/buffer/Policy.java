package simpledb.buffer;

public enum Policy {
	leastRecentUsed,
	clock,
	defaultPolicy;
	
	public static Policy fromString(String str){
		if(str.equals("leastRecentUsed")){
			return leastRecentUsed;
		}else if(str.equals("clock")){
			return clock;
		}else{
			return defaultPolicy;
		}
	}
}
