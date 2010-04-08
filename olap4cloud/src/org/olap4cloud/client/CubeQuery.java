package org.olap4cloud.client;

import java.util.ArrayList;
import java.util.List;

public class CubeQuery {
	
	List<CubeQueryCondition> conditions = new ArrayList<CubeQueryCondition>();
	
	List<CubeQueryAggregate> aggregates = new ArrayList<CubeQueryAggregate>();
	
	public CubeQuery() {
		
	}

	public List<CubeQueryCondition> getConditions() {
		return conditions;
	}
	
	public List<CubeQueryAggregate> getAggregates() {
		return aggregates;
	}
}
