package test;

import java.util.ArrayList;
import java.util.Random;

import simpledb.buffer.Policy;
import simpledb.planner.Planner;
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

public class LRUTest {
	public static void main(String[] args) {
		try {
			// analogous to the driver
			SimpleDB.init("studentdb", Policy.leastRecentUsed);

			// analogous to the connection
			Transaction tx;

			tx = new Transaction();
			String studentTable = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			SimpleDB.planner().executeUpdate(studentTable, tx);
			tx.commit();

			// analogous to the statement

			String insertStudents = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";

			ArrayList<Integer> majorIdList = new ArrayList<Integer>();
			majorIdList.add(10);
			majorIdList.add(20);
			majorIdList.add(30);

			String studentData = new String();

			Random r = new Random(0);
			for (int i = 0; i < 90; i++) {
				tx = new Transaction();
				studentData = "(" + i + ", " + "'a" + i + "', "
						+ majorIdList.get(r.nextInt(3)) + ", " + 2000 +  + r.nextInt(15) + ")";
				SimpleDB.planner().executeUpdate(insertStudents + studentData, tx);
				tx.commit();
			}

			String qry = "select SName from STUDENT";
			tx = new Transaction();
			SimpleDB.bufferMgr().enableDebug();
			SimpleDB.planner().createQueryPlan(qry, tx);
			tx.commit();
			
			/*
			Planner p;
			Scan s;
			Plan plan;
			
			String previousBufferOutput="";
			tx = new Transaction();
			p = SimpleDB.planner();
			for(int i = 0; i < 3; i++){
				plan = p.createQueryPlan(qry, tx);

				System.out.println("-----QUery------");
							// analogous to the result set
				s = plan.open();
	
				while (s.next()) {
					String bufferOutput = SimpleDB.bufferMgr().getBufferMgrInfo();
					System.out.println(bufferOutput);
					if(!previousBufferOutput.equals(bufferOutput)){
						System.out.println(bufferOutput);
						previousBufferOutput = bufferOutput;
					}
				}
				s.close();
			}*/
			//tx.commit();
			
			tx = new Transaction();
			SimpleDB.planner().executeUpdate("DELETE FROM STUDENT", tx);
			tx.commit();
			
			//tx = new Transaction();
			//SimpleDB.planner().executeUpdate("DROP TABLE STUDENT", tx);
			//tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
