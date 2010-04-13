package org.olap4cloud.test;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQuery;
import org.olap4cloud.client.CubeQueryAggregate;
import org.olap4cloud.client.CubeQueryCondition;
import org.olap4cloud.client.CubeQueryResult;
import org.olap4cloud.client.CubeQueryResultRow;
import org.olap4cloud.client.OLAPEngine;
import org.olap4cloud.impl.SumCubeScanAggregate;

public class OLAPEngineTest {
	
	public static void main(String[] args) throws Exception {
		executeQueryTest();
	}
	
	static void executeQueryTest() throws Exception {
		CubeDescriptor cubeDescriptor = TestCubeUtils.createTestCubeDescriptor();
		CubeQuery cubeQuery = new CubeQuery();
		CubeQueryCondition condition = new CubeQueryCondition("d1");
		condition.getDimensionValues().add(1l);
		cubeQuery.getConditions().add(condition);
		cubeQuery.getAggregates().add(new CubeQueryAggregate("sum(m1)"));
//		cubeQuery.getGroupBy().add("d2");
//		cubeQuery.getGroupBy().add("d3");
		OLAPEngine olapEngine = new OLAPEngine();
		CubeQueryResult r = olapEngine.executeQuery(cubeQuery, cubeDescriptor);
		for(CubeQueryResultRow row: r.getRows()) {
			for(long l: row.getGroupBy())
				System.out.print(l + "\t");
			for(double d: row.getValues())
				System.out.print(d + "\t");
			System.out.println();
		}
	}
}
