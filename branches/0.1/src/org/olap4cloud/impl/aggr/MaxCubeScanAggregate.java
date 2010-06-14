package org.olap4cloud.impl.aggr;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.OLAPEngineException;
import org.olap4cloud.impl.CubeScanAggregate;

public class MaxCubeScanAggregate extends CubeScanAggregate {
	double value;
	
	public MaxCubeScanAggregate(String s, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		super(s, cubeDescriptor);
	}
	
	@Override
	public void collect(double v) {
		if(v > value)
			value = v;
	}

	@Override
	public double getResult() {
		return value;
	}

	@Override
	public void reset() {
		value = Double.NEGATIVE_INFINITY;
	}

}
