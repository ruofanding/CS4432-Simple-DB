package test.project2;

import java.util.ArrayList;
import java.util.Date;
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
 * CS4432-Project1: Test for Clock policy
 *
 */
public class IndexTester {
	private static String getRandomName(Random r) {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < 7; i++) {
			s.append((char) (r.nextInt(26) + 'a'));
		}
		return s.toString();
	}

	public static void main(String[] args) {
		try {
			int SIZE = 100;
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

			tx = new Transaction();
			SimpleDB.planner().executeUpdate(
					"create eh index myIndex on STUDENT (SId)", tx);

			tx.commit();

			// analogous to the statement

			String insertStudents = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";

			ArrayList<Integer> majorIdList = new ArrayList<Integer>();
			majorIdList.add(10);
			majorIdList.add(20);
			majorIdList.add(30);

			String studentData = new String();
			SimpleDB.bufferMgr().disableDebug();
			Planner pp = SimpleDB.planner();
			Random r = new Random(0);
			for (int i = 0; i < SIZE; i++) {
				tx = new Transaction();
				names[i] = getRandomName(r);
				years[i] = 2000 + r.nextInt(15);
				majors[i] = majorIdList.get(r.nextInt(3));
				studentData = "(" + i + ", '" + names[i] + "', " + majors[i]
						+ ", " + years[i] + ")";
				pp.executeUpdate(insertStudents + studentData,
						tx);
				tx.commit();
			}

			String qry = "select SId, SName, MajorId, GradYear from STUDENT where SId = ";

			Planner p;
			Scan s;
			Plan plan;

			Date start = new Date();
			boolean matched = true;
			System.out.println("-----QUery------");
			tx = new Transaction();
			for (int i = 0; i < SIZE; i++) {
				p = SimpleDB.planner();
				plan = p.createQueryPlan(qry + i, tx);

				s = plan.open();

				if (s.next()) {
					if (!s.getString("sname").equals(names[i])) {
						matched = false;
						System.out.println("Name does not match");
					}
					if (!(s.getInt("majorid") == majors[i])) {
						matched = false;
						System.out.println("Major does not match");
					}
					if (!(s.getInt("gradyear") == years[i])) {
						matched = false;
						System.out.println("Year does not match");
					}
				} else {
					matched = false;
				}
				s.close();
			}
			tx.commit();

			System.out.println("---------Result----------");
			if (matched) {
				System.out.println("All selected data match inserted data");
			} else {
				System.out.println("Data not matched");
			}
			System.out.println(start + " " + new Date());

			tx = new Transaction();
			SimpleDB.planner().executeUpdate("DELETE FROM STUDENT", tx);
			tx.commit();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
