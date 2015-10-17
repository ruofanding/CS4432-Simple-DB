package test.project2;

import java.util.Date;
import java.util.Random;

import simpledb.buffer.Policy;
import simpledb.planner.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class PerformanceTestInsert {
	final static int maxSize = 100000;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Transaction tx;
		Planner p;

		//Initialize database
		SimpleDB.init("performancedb", Policy.clock);

		Random rand = null;
		try {
			tx = new Transaction();
			p = SimpleDB.planner();
			p.executeUpdate("Create table test1" + "( a1 int," + "  a2 int"
					+ ")", tx);
			p.executeUpdate("Create table test2" + "( a1 int," + "  a2 int"
					+ ")", tx);
			p.executeUpdate("Create table test3" + "( a1 int," + "  a2 int"
					+ ")", tx);
			p.executeUpdate("Create table test4" + "( a1 int," + "  a2 int"
					+ ")", tx);
			p.executeUpdate("Create table test5" + "( a1 int," + "  a2 int"
					+ ")", tx);

			p.executeUpdate("create sh index idx1 on test1 (a1)", tx);
			p.executeUpdate("create eh index idx2 on test2 (a1)", tx);
			p.executeUpdate("create bt index idx3 on test3 (a1)", tx);
			tx.commit();
			for (int i = 1; i <= 5; i++) {
				tx = new Transaction();
				if (i != 5) {
					rand = new Random(1);// ensure every table gets the same
											// data
					int percentage = 0;
					for (int j = 0; j < maxSize; j++) {
						if(j > maxSize * percentage / 100){
							percentage++;
							System.out.println("Test" + i + " insertion finishes " + percentage + "% at " + (new Date()));
						}
						p.executeUpdate("insert into test" + i
								+ " (a1,a2) values(" + rand.nextInt(100000) + ","
								+ rand.nextInt(100000) + ")", tx);
					}
				} else// case where i=5
				{
					for (int j = 0; j < maxSize / 2; j++)// insert 10000 records
															// into test5
					{
						p.executeUpdate("insert into test" + i
								+ " (a1,a2) values(" + j + "," + j + ")", tx);
					}
				}
				tx.commit();
			}
			tx.commit();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
