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
public class ExtensibleIndexTester {
	private static String getRandomName(Random r) {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < 7; i++) {
			s.append((char) (r.nextInt(26) + 'a'));
		}
		return s.toString();
	}

	public static void main(String[] args) {
		try {
			int SIZE = 10000;
			int[] id = new int[SIZE];
			String[] names = new String[SIZE];
			int[] majors = new int[SIZE];
			int[] years = new int[SIZE];

			Transaction tx;
			Planner p;
			Scan s;
			Plan plan;
			Random r = new Random(0);

			//Initialize database
			SimpleDB.init("studentdb", Policy.clock);

			//Create table
			tx = new Transaction();
			String studentTable = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			SimpleDB.planner().executeUpdate(studentTable, tx);
			tx.commit();

			//Create index
			
			tx = new Transaction();
			SimpleDB.planner().executeUpdate(
					"create eh index myIndex on STUDENT (SId)", tx);
			tx.commit();

			//Insert data
			String insertStudents = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			SimpleDB.bufferMgr().disableDebug();
			p = SimpleDB.planner();
			for (int i = 0; i < SIZE; i++) {
				tx = new Transaction();
				id[i] = r.nextInt(SIZE);
				names[i] = getRandomName(r);
				years[i] = 2000 + r.nextInt(15);
				majors[i] = (r.nextInt(3) + 1) * 10;
				String studentData = "(" + id[i] + ", '" + names[i] + "', " + majors[i]
						+ ", " + years[i] + ")";
				
				p.executeUpdate(insertStudents + studentData, tx);
				tx.commit();
			}

			SimpleDB.fileMgr().initIOCounter();
			//Select data
			System.out.println("-----QUery------");
			String qry = "select SId, SName, MajorId, GradYear from STUDENT where SId = ";

			boolean matched = true;
			for (int i = 0; i < SIZE; i++) {
				tx = new Transaction();
				p = SimpleDB.planner();
				plan = p.createQueryPlan(qry + id[i], tx);
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
				tx.commit();
			}
			System.out.println(SimpleDB.fileMgr().getReadCounter() + " " + SimpleDB.fileMgr().getWriteCounter());
			tx.commit();

			System.out.println("---------Result----------");
			if (matched) {
				System.out.println("All selected data match inserted data");
			} else {
				System.out.println("Data not matched");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
