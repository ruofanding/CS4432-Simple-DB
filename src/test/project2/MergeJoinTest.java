package test.project2;

import java.util.ArrayList;
import java.util.Collections;

import simpledb.buffer.Policy;
import simpledb.planner.Planner;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class MergeJointTest {
	static int SIZE = 10;
	public static void main(String[] argv){
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
			System.out.println(i + " " + values.get(i));
			p.executeUpdate("insert into test1 (a1, a2) values ("+ i + ", " + values.get(i) +")", tx);
		}
		tx.commit();

		Collections.shuffle(values);
		tx = new Transaction();
		System.out.println("Insert test2");
		for(int i = 0; i < values.size(); i++){
			System.out.println(i + " " + values.get(i));
			p.executeUpdate("insert into test2 (b1, b2) values ("+ i + ", " + values.get(i) +")", tx);
		}
		tx.commit();

		Plan plan = p.createQueryPlan("Select a1, b1 from test1, test2 Where a2 = b2", tx);
		Scan scan = plan.open();
		while (scan.next()) {
			System.out.println(scan.getInt("a1") + " " + scan.getInt("b1"));
		}
		tx.commit();
	}
}
