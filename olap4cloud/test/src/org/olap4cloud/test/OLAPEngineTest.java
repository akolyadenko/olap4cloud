package org.olap4cloud.test;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQuery;
import org.olap4cloud.client.CubeQueryCondition;
import org.olap4cloud.client.OLAPEngine;

public class OLAPEngineTest {
	
	public static void main(String[] args) throws Exception {
		executeQueryTest();
	}
	
	static void executeQueryTest() throws Exception {
		CubeDescriptor cubeDescriptor = TestCubeUtils.createTestCubeDescriptor();
		CubeQuery cubeQuery = new CubeQuery();
		CubeQueryCondition condition = new CubeQueryCondition("d2");
		condition.getDimensionValues().add(602l);
		cubeQuery.getConditions().add(condition);
		OLAPEngine olapEngine = new OLAPEngine();
		olapEngine.executeQuery(cubeQuery, cubeDescriptor);
	}
}
