package org.olap4cloud.impl;

import org.apache.hadoop.hbase.util.Pair;
import org.olap4cloud.client.CubeDescriptor;

public class SumCubeScanAggregate extends CubeScanAggregate {

	double value = 0;
	
	public SumCubeScanAggregate(String s, CubeDescriptor cubeDescriptor) {
		super(s, cubeDescriptor);
	}
	
	@Override
	public void collect(double v) {
		value += v;		
	}

	@Override
	public double getResult() {
		return value;
	}

	@Override
	public void reset() {
		value = 0;
	}

}
