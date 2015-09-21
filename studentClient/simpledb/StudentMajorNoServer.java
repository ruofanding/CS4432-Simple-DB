 import simpledb.buffer.Policy;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/* This is a version of the StudentMajor program that
 * accesses the SimpleDB classes directly (instead of
 * connecting to it as a JDBC client).  You can run it
 * without having the server also run.
 * 
 * These kind of programs are useful for debugging
 * your changes to the SimpleDB source code.
 */

public class StudentMajorNoServer {
	public static void main(String[] args) {
		try {
			// analogous to the driver
			SimpleDB.init("studentdb", Policy.leastRecentUsed);

			// analogous to the connection
			Transaction tx = new Transaction();

			String ss = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			SimpleDB.planner().executeUpdate(ss, tx);
			
			// analogous to the statement
			String qry = "select SName " + "from STUDENT";
			
			Plan p = SimpleDB.planner().createQueryPlan(qry, tx);

			// analogous to the result set
			Scan s = p.open();

			System.out.println("Name\tMajor");
			while (s.next()) {
				String sname = s.getString("sname"); // SimpleDB stores field
														// names
				String dname = s.getString("dname"); // in lower case
				System.out.println(sname + "\t" + dname);
			}
			
			System.out.println(SimpleDB.bufferMgr().getBufferMgrInfo());
			s.close();
			tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
