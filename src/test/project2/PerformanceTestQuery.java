package test.project2;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import simpledb.buffer.Policy;
import simpledb.file.FileMgr;
import simpledb.planner.Planner;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class PerformanceTestQuery {
	final static int maxSize = 20000;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Transaction tx;
		Planner p;

		// Initialize database
		SimpleDB.init("performancedb", Policy.clock);

		FileMgr fileManager = SimpleDB.fileMgr();
		Transaction.disablePrint();
		long start;
		Random rand = new Random(2);
		List<Integer> testValues = new LinkedList<Integer>();
		for (int i = 0; i < 50; i++) {
			testValues.add(rand.nextInt(100000));
		}

		for (int i = 1; i <= 4; i++) {
			System.out.println("Test" + i + "\n-------------------\nRead/Write/Time(usec)");
			for (Integer val : testValues) {
				fileManager.initIOCounter();
				start = System.nanoTime();

				p = SimpleDB.planner();
				tx = new Transaction();
				Plan plan = p.createQueryPlan("Select a1, a2 from test" + i
						+ " Where a1 = " + val, tx);
				Scan scan = plan.open();
				while (scan.next()) {
					;
				}
				tx.commit();
				System.out.println(fileManager.getReadCounter() + ", "
						+ fileManager.getWriteCounter() + ", "
						+ (System.nanoTime() - start) / 1000);
			}
			System.out.println();
		}
	}
}
