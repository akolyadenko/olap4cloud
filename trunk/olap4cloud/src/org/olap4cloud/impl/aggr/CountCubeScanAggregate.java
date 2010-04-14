package org.olap4cloud.impl.aggr;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.OLAPEngineException;
import org.olap4cloud.impl.CubeScanAggregate;

public class CountCubeScanAggregate extends CubeScanAggregate {
	
	double value;
	
	public CountCubeScanAggregate(String s, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		super(s, cubeDescriptor);
	}
	
	@Override
	public void collect(double v) {
		v += 1;
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
