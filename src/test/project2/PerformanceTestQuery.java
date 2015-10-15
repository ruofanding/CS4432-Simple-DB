package test.project2;

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
	public static void main(String[] args) throws Exception{
		Transaction tx;
		Planner p;

		//Initialize database
		SimpleDB.init("performancedb", Policy.clock);

		FileMgr fileManager = SimpleDB.fileMgr();
		Random rand = null;
			for(int i = 1; i <= 5; i++){
				p = SimpleDB.planner();
				tx = new Transaction();

				fileManager.initIOCounter();
				Plan plan = p.createQueryPlan("Select a1, a2 from test" + i + " Where a1 = 42", tx);
				Scan scan = plan.open();
				while(scan.next()){
					;
				}
				tx.commit();
				System.out.println("Test" + i + ": " + fileManager.getReadCounter() + " " + fileManager.getWriteCounter());
			}
	}
}
