package org.olap4cloud.impl;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.OLAPEngineException;

public class GenerateAggregationCubeMR {

	static Logger logger = Logger.getLogger(GenerateAggregationCubeMR.class);
	
	public static void generateCube(AggregationCubeDescriptor aggCube,
			CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		try {
			createTable(aggCube);
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new OLAPEngineException(e);
		}
	}

	private static void createTable(AggregationCubeDescriptor cubeDescriptor) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor(cubeDescriptor.getCubeDataTable());
		for (int i = 0; i < cubeDescriptor.getMeasures().size(); i++)
			tableDescr.addFamily(new HColumnDescriptor(	OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX
									+ cubeDescriptor.getMeasures().get(i)
											.getName()));
		if (admin.tableExists(cubeDescriptor.getCubeDataTable())) {
			admin.disableTable(cubeDescriptor.getCubeDataTable());
			admin.deleteTable(cubeDescriptor.getCubeDataTable());
		}
		admin.createTable(tableDescr);
	}

}
