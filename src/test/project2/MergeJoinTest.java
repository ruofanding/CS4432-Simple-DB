package test.project2;

import java.util.ArrayList;
import java.util.Collections;

import simpledb.buffer.Policy;
import simpledb.planner.Planner;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class MergeJoinTest {
	static int SIZE = 10;
	public static void main(String[] argv){
		Transaction.disablePrint();
		System.out.println("Commit Info is hidden");
		SimpleDB.init("mergesorttest", Policy.clock);

		Planner p = SimpleDB.planner();
		Transaction tx;

		tx = new Transaction();
		p = SimpleDB.planner();

		p.executeUpdate("Create table test1 (a1 int, a2 int)", tx);
		p.executeUpdate("Create table test2 (b1 int, b2 int)", tx);
		tx.commit();

		
		ArrayList<Integer> values = new ArrayList<Integer>();
		for(int i = 0; i < SIZE; i++){
			values.add(i);
		}
		tx = new Transaction();
		System.out.println("Insert test1");
		for(int i = 0; i < values.size(); i++){
			System.out.println("a1=" + i + ", a2=" + values.get(i));
			p.executeUpdate("insert into test1 (a1, a2) values ("+ i + ", " + values.get(i) +")", tx);
		}
		tx.commit();

		Collections.shuffle(values);
		tx = new Transaction();
		System.out.println("Insert test2");
		for(int i = 0; i < values.size(); i++){
			System.out.println("b1=" + i + ", b2=" + values.get(i));
			p.executeUpdate("insert into test2 (b1, b2) values ("+ i + ", " + values.get(i) +")", tx);
		}
		tx.commit();

		tx = new Transaction();
		String qry = "Select a1, b1 from test1, test2 Where a2 = b2";
		Plan plan = p.createQueryPlan("Select a1, b1, a2, b2 from test1, test2 Where a2 = b2", tx);
		Scan scan = plan.open();
		System.out.println(qry);
		while (scan.next()) {
			System.out.println("a1=" + scan.getInt("a1") + ", b1=" + scan.getInt("b1") + ", a2=b2=" + scan.getInt("b2"));
		}
		tx.commit();

		qry = "Select b1, b2 from test2";
		plan = p.createQueryPlan(qry, tx);
		scan = plan.open();
		System.out.println(qry);
		while (scan.next()) {
			System.out.println("b1=" + scan.getInt("b1") + ", b2=" + scan.getInt("b2"));
		}
		tx.commit();

		tx = new Transaction();
		System.out.println("-----Do joint one more time!");
		qry = "Select a1, b1 from test1, test2 Where a2 = b2";
		plan = p.createQueryPlan("Select a1, b1, a2, b2 from test1, test2 Where a2 = b2", tx);
		scan = plan.open();
		System.out.println(qry);
		while (scan.next()) {
			System.out.println("a1=" + scan.getInt("a1") + ", b1=" + scan.getInt("b1") + ", a2=b2=" + scan.getInt("b2"));
		}
		tx.commit();

	}
}
