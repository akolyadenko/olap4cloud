package org.olap4cloud.test;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeDimension;
import org.olap4cloud.client.CubeMeasure;
import org.olap4cloud.impl.GenerateCubeIndexMR;
import org.olap4cloud.impl.GenerateCubeMR;

public class TestCubeUtils {

	public static void generateTestCube() throws Exception {
		CubeDescriptor descr = TestCubeUtils.createTestCubeDescriptor();
		GenerateCubeMR.generateCube(descr);
		GenerateCubeIndexMR.generate(descr);
	}
	
	public static CubeDescriptor createTestCubeDescriptor() {
		CubeDescriptor descr = new CubeDescriptor();
		descr.setSourceTable("testfacttable");
		descr.setCubeName("testcube");
		descr.getDimensions().add(new CubeDimension("data.d1", "d1"));
		descr.getDimensions().add(new CubeDimension("data.d2", "d2"));
		descr.getDimensions().add(new CubeDimension("data.d3", "d3"));
		descr.getMeasures().add(new CubeMeasure("data.m1", "m1"));
		descr.getMeasures().add(new CubeMeasure("data.m2", "m2"));
		descr.getMeasures().add(new CubeMeasure("data.m3", "m3"));
		return descr;
	}

	public static void main(String args[]) throws Exception {
		generateTestCube();
	}
}
