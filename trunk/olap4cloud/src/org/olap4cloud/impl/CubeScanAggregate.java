package org.olap4cloud.impl;

import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeDimension;

public abstract class CubeScanAggregate {
	
	Pair<byte[], byte[]> column;
	
	int columnNumber = -1;
	
	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}

	public CubeScanAggregate(String aggregate, CubeDescriptor cubeDescriptor) {
		StringTokenizer st = new StringTokenizer(aggregate, "()", false);
		st.nextToken();
		String dimName = st.nextToken();
		for(CubeDimension dimension: cubeDescriptor.getDimensions()) 
			if(dimName.equals(dimension.getName())) {
				StringTokenizer st2 = new StringTokenizer(dimension.getSourceField(), ".", false);
				String family = st2.nextToken();
				String columnName = st2.nextToken();
				column = new Pair<byte[], byte[]>(Bytes.toBytes(family), Bytes.toBytes(columnName));
				break;
			}
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
