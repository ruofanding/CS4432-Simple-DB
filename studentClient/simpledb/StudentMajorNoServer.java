 import java.util.ArrayList;
import java.util.Collections;

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

			String studentTable = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			SimpleDB.planner().executeUpdate(studentTable, tx);
			
			// analogous to the statement
			
			String insertStudents = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			String[] studvals = { "(1, 'joe', 10, 2004)",
					"(2, 'amy', 20, 2004)", "(3, 'max', 10, 2005)",
					"(4, 'sue', 20, 2005)", "(5, 'bob', 30, 2003)",
					"(6, 'kim', 20, 2001)", "(7, 'art', 30, 2004)",
					"(8, 'pat', 20, 2001)", "(9, 'lee', 20, 2004)" };
			
			ArrayList<Integer>  majorIdList = new ArrayList<Integer>();
			majorIdList.add(10);
			majorIdList.add(20);
			majorIdList.add(30);

			Collections.shuffle(majorIdList);
			String studentData = new String();
			
			for(int i = 0; i < 1000; i++)
			{
				Collections.shuffle(majorIdList);
				studentData = insertStudents + "("+ i + ", " + "'a" + i + "', " + majorIdList.get(0) + ", 2004)";
				SimpleDB.planner().executeUpdate(insertStudents + studvals, tx);
			}
			
			String qry = "select SName " + "from STUDENT";
			Plan p = SimpleDB.planner().createQueryPlan(qry, tx);

			// analogous to the result set
			Scan s = p.open();

			System.out.println("Name\tMajor");
			while (s.next()) {
				String sname = s.getString("sname"); // SimpleDB stores field
														// names
				//String dname = s.getString("dname"); // in lower case
				System.out.println(sname + "\t");
			}
			
			System.out.println(SimpleDB.bufferMgr().getBufferMgrInfo());
			s.close();
			tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
