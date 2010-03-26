package org.olap4cloud.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.olap4cloud.impl.CubeIndexEntry;
import org.olap4cloud.impl.CubeScan;


public class OLAPEngine {
	public CubeQueryResult executeQuery(CubeQuery query, CubeDescriptor descriptor) {
		return null;
	}
	
	CubeScan getCubeScan(CubeQuery query, CubeDescriptor cubeDescriptor) {
		CubeScan scan = new CubeScan();
		List<CubeIndexEntry> index = null;
		for(CubeQueryCondition condition: query.getConditions()) {
			String dimensionName = condition.getDimensionName();
			int dimensionNumber = getDimensionNumber(dimensionName, cubeDescriptor);
			List<CubeIndexEntry> dimIndex = new ArrayList<CubeIndexEntry>();
			for(long dimVal: condition.dimensionValues) {
				List<CubeIndexEntry> dimValIndex = getIndexForDimensionValue(dimensionNumber, dimVal);
				dimIndex = mergeIndexes(dimIndex, dimValIndex);
			}
			if(index == null)
				index = dimIndex;
			else
				index = joindIndexes(index, dimIndex);
		}
		for(CubeIndexEntry indexEntry: index) {
			byte startRow[] = getStartRow(index, cubeDescriptor.dimensions.size());
			byte stopRow[] = getStopRow(index, cubeDescriptor.dimensions.size());
			Pair<byte[], byte[]> range = new Pair<byte[], byte[]>(startRow, stopRow);
		}
		return scan;
	}

	private int getDimensionNumber(String dimensionName,
			CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		for(int i = 0; i < cubeDescriptor.dimensions.size(); i ++) {
			if(cubeDescriptor.dimensions.get(i).getName().equals(dimensionName))
				return i + 1;
		}
		throw new OLAPEngineException("Can't find dimension " + dimensionName);
	}
	
	
}
