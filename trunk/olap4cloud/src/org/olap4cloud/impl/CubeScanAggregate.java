package org.olap4cloud.impl;

import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeDimension;
import org.olap4cloud.client.CubeMeasure;
import org.olap4cloud.client.OLAPEngineException;

public abstract class CubeScanAggregate {
	
	static Logger logger = Logger.getLogger(CubeScanAggregate.class);
	
	Pair<byte[], byte[]> column;
	
	int columnNumber = -1;
	
	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}

	public CubeScanAggregate(String aggregate, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		String methodName = "constructor() ";
		if(logger.isDebugEnabled()) logger.debug(methodName + "aggregate = " + aggregate);
		StringTokenizer st = new StringTokenizer(aggregate, "()", false);
		st.nextToken();
		String measureName = st.nextToken();
		if(logger.isDebugEnabled()) logger.debug(methodName + "measureName = " + measureName);
		for(CubeMeasure measure: cubeDescriptor.getMeasures()) {
			if(logger.isDebugEnabled()) logger.debug(methodName + "measure.getName() = " + measure.getName());
			if(measureName.equals(measure.getName())) {
				StringTokenizer st2 = new StringTokenizer(measure.getSourceField(), ".", false);
				String family = st2.nextToken();
				String columnName = st2.nextToken();
				column = new Pair<byte[], byte[]>(Bytes.toBytes(family), Bytes.toBytes(columnName));
				break;
			}
		}
		throw new OLAPEngineException("Invalid measure in " + aggregate);
	}
	
	public Pair<byte[], byte[]> getColumn() {
		return column;
	}
	
	public int getColumnNuber() {
		return columnNumber;
	}
	
	public int getColumnNumber() {
		return columnNumber;
	}
	
	public abstract void reset();
	
	public abstract double getResult();
	
	public abstract void collect(double v);
}
