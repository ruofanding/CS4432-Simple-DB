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


/**
 * CS4432-Project1:
 * Test for Clock policy
 *
 */
public class ClockTest {
	private static String getRandomName(Random r) {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < 7; i++) {
			s.append((char) (r.nextInt(26) + 'a'));
		}
		return s.toString();
	}

	public static void main(String[] args) {
		try {
			int SIZE = 200;
			String[] names = new String[SIZE];
			int[] majors = new int[SIZE];
			int[] years = new int[SIZE];

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

			ArrayList<Integer> majorIdList = new ArrayList<Integer>();
			majorIdList.add(10);
			majorIdList.add(20);
			majorIdList.add(30);

			String studentData = new String();

			Random r = new Random(0);
			for (int i = 0; i < SIZE; i++) {
				tx = new Transaction();
				names[i] = getRandomName(r);
				years[i] = 2000 + r.nextInt(15);
				majors[i] = majorIdList.get(r.nextInt(3));
				studentData = "(" + i + ", '" + names[i] + "', " + majors[i]
						+ ", " + years[i] + ")";
				SimpleDB.planner().executeUpdate(insertStudents + studentData,
						tx);
				tx.commit();
			}

			String qry = "select SId, SName, MajorId, GradYear from STUDENT";
			SimpleDB.bufferMgr().enableDebug();

			Planner p;
			Scan s;
			Plan plan;

			tx = new Transaction();
			p = SimpleDB.planner();
			plan = p.createQueryPlan(qry, tx);

			System.out.println("-----QUery------");
			// analogous to the result set
			s = plan.open();

			boolean matched = true;
			while (s.next()) {
				if (!s.getString("sname").equals(names[s.getInt("sid")])) {
					matched = false;
					System.out.println("Name does not match");
				}
				if (!(s.getInt("majorid") == majors[s.getInt("sid")])) {
					matched = false;
					System.out.println("Major does not match");
				}
				if (!(s.getInt("gradyear") == years[s.getInt("sid")])) {
					matched = false;
					System.out.println("Year does not match");
				}
			}			
			s.close();
			tx.commit();
			


			SimpleDB.bufferMgr().disableDebug();
			tx = new Transaction();
			SimpleDB.planner().executeUpdate("DELETE FROM STUDENT", tx);
			tx.commit();
			
			System.out.println("---------Result----------");
			if(matched){
				System.out.println("All selected data match inserted data");
			}else{
				System.out.println("Data not matched");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
