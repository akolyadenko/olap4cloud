package org.olap4cloud.impl;

import org.apache.hadoop.mapreduce.Job;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQueryResult;

public class CubeScanMR {
	public static CubeQueryResult scan(CubeScan scan, CubeDescriptor cubeDescriptor) throws Exception {
		Job job = new Job();
		job.setJarByClass(CubeScanMR.class);
		return null;
	}
}
