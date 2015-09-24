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
			SimpleDB.init("studentdb", Policy.clock);

			// analogous to the connection
			Transaction tx;

			tx = new Transaction();
			String studentTable = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			SimpleDB.planner().executeUpdate(studentTable, tx);
			tx.commit();
			
			// analogous to the statement
			
			String insertStudents = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			
			ArrayList<Integer>  majorIdList = new ArrayList<Integer>();
			majorIdList.add(10);
			majorIdList.add(20);
			majorIdList.add(30);

			String studentData = new String();
			
			for(int i = 0; i < 1000; i++)
			{
				tx = new Transaction();
				studentData = "("+ i + ", " + "'a" + i + "', " + majorIdList.get(i%3) + ", 2004)";
				SimpleDB.planner().executeUpdate(insertStudents + studentData, tx);
				tx.commit();
			}
			

			String qry = "select SName " + "from STUDENT";

			
			System.out.println("-----QUery------");
			Plan p = SimpleDB.planner().createQueryPlan(qry, tx);

			// analogous to the result set
			Scan s = p.open();

			System.out.println(p.blocksAccessed());
			System.out.println(p.recordsOutput());
			//System.out.println("Name\tMajor");
			
			while (s.next()) {
				String sname = s.getString("sname"); // SimpleDB stores field
														// names
				//String dname = s.getString("dname"); // in lower case
				//System.out.println(sname + "\t");
			}
			s.close();
			
			
			p = SimpleDB.planner().createQueryPlan(qry, tx);

			// analogous to the result set
			s = p.open();

			System.out.println(p.blocksAccessed());
			System.out.println(p.recordsOutput());
			//System.out.println("Name\tMajor");
			
			while (s.next()) {
				String sname = s.getString("sname"); // SimpleDB stores field
														// names
				//String dname = s.getString("dname"); // in lower case
				//System.out.println(sname + "\t");
			}
			s.close();
			
			
			System.out.println(SimpleDB.bufferMgr().getBufferMgrInfo());

			tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
