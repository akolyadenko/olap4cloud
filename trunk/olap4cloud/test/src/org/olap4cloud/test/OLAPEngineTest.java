package org.olap4cloud.test;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQuery;
import org.olap4cloud.client.CubeQueryAggregate;
import org.olap4cloud.client.CubeQueryCondition;
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
		olapEngine.executeQuery(cubeQuery, cubeDescriptor);
	}
}
