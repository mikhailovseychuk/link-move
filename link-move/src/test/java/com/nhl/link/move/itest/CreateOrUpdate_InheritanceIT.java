package com.nhl.link.move.itest;

import com.nhl.link.move.Execution;
import com.nhl.link.move.LmTask;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.unit.LmIntegrationTest;
import com.nhl.link.move.unit.cayenne.ti.TiSub1;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateOrUpdate_InheritanceIT extends LmIntegrationTest {

	@Test
	public void test_Subclass_MatchBySubKey() {

		LmTask task = etl.service(ITaskService.class)
				.createOrUpdate(TiSub1.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_sub1_to_ti_sub1_sub_key.xml")
				.matchBy(TiSub1.SUB_KEY)
				.task();

		srcRunSql("INSERT INTO \"UTEST\".\"ETL_SUB1\" (\"S_KEY\", \"S_SUBP1\") VALUES ('a', 'p1')");
		srcRunSql("INSERT INTO \"UTEST\".\"ETL_SUB1\" (\"S_KEY\", \"S_SUBP1\") VALUES ('b', 'p2')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from \"utest\".\"ti_super\""));
		assertEquals(2, targetScalar("SELECT count(1) from \"utest\".\"ti_super\" WHERE \"type\" = 'sub1'"));

		assertEquals(2, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\""));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" = 'a' AND \"subp1\" = 'p1'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" = 'b' AND \"subp1\" = 'p2'"));

		srcRunSql("INSERT INTO \"UTEST\".\"ETL_SUB1\" (\"S_KEY\", \"S_SUBP1\") VALUES ('c', NULL)");
		targetRunSql("UPDATE \"utest\".\"ti_sub1\" SET \"subp1\" = 'p3' WHERE \"subp1\" = 'p1'");

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		assertEquals(3, targetScalar("SELECT count(1) from \"utest\".\"ti_super\""));
		assertEquals(3, targetScalar("SELECT count(1) from \"utest\".\"ti_super\" WHERE \"type\" = 'sub1'"));

		assertEquals(3, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\""));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" = 'a' AND \"subp1\" = 'p1'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" = 'b' AND \"subp1\" = 'p2'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" = 'c' AND \"subp1\" IS NULL"));

		srcRunSql("DELETE FROM \"UTEST\".\"ETL_SUB1\" WHERE \"S_KEY\" = 'a'");

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}

	@Test
	public void test_Subclass_MatchBySuperKey() {

		LmTask task = etl.service(ITaskService.class)
				.createOrUpdate(TiSub1.class)
				.sourceExtractor("com/nhl/link/move/itest/etl1_sub1_to_ti_sub1_super_key.xml")
				.matchBy(TiSub1.SUPER_KEY)
				.task();

		srcRunSql("INSERT INTO \"UTEST\".\"ETL_SUB1\" (\"S_KEY\", \"S_SUBP1\") VALUES ('a', 'p1')");
		srcRunSql("INSERT INTO \"UTEST\".\"ETL_SUB1\" (\"S_KEY\", \"S_SUBP1\") VALUES ('b', 'p2')");

		Execution e1 = task.run();
		assertExec(2, 2, 0, 0, e1);
		assertEquals(2, targetScalar("SELECT count(1) from \"utest\".\"ti_super\""));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'a'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'b'"));

		assertEquals(2, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\""));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p1'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p2'"));

		srcRunSql("INSERT INTO \"UTEST\".\"ETL_SUB1\" (\"S_KEY\", \"S_SUBP1\") VALUES ('c', NULL)");
		targetRunSql("UPDATE \"utest\".\"ti_sub1\" SET \"subp1\" = 'p3' WHERE \"subp1\" = 'p1'");

		Execution e2 = task.run();
		assertExec(3, 1, 1, 0, e2);
		assertEquals(3, targetScalar("SELECT count(1) from \"utest\".\"ti_super\""));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'a'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'b'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_super\" WHERE \"type\" = 'sub1' AND \"super_key\" = 'c'"));

		assertEquals(3, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\""));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p1'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" = 'p2'"));
		assertEquals(1, targetScalar("SELECT count(1) from \"utest\".\"ti_sub1\" WHERE \"sub_key\" is null AND \"subp1\" is null"));

		srcRunSql("DELETE FROM \"UTEST\".\"ETL_SUB1\" WHERE \"S_KEY\" = 'a'");

		Execution e3 = task.run();
		assertExec(2, 0, 0, 0, e3);

		Execution e4 = task.run();
		assertExec(2, 0, 0, 0, e4);
	}

}
