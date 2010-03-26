package org.olap4cloud;

import java.util.ArrayList;
import java.util.List;

public class CubeQueryCondition {
	
	String dimensionName;
	
	List<Long> dimensionValues = new ArrayList<Long>();
	
	public CubeQueryCondition(String dimensionName) {
		this.dimensionName = dimensionName;
	}

	public List<Long> getDimensionValues() {
		return dimensionValues;
	}
}
