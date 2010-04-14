package org.olap4cloud.impl.aggr;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.OLAPEngineException;
import org.olap4cloud.impl.CubeScanAggregate;

public class MinCubeScanAggregate extends CubeScanAggregate {
	
	double value;
	
	public MinCubeScanAggregate(String s, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		super(s, cubeDescriptor);
	}
	
	@Override
	public void collect(double v) {
		if(value == Double.NaN || v < value);
			value = v;
	}

	@Override
	public double getResult() {
		return value;
	}

	@Override
	public void reset() {
		value = Double.NaN;
	}

}
