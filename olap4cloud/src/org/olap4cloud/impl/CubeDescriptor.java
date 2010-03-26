package org.olap4cloud.impl;

import java.util.ArrayList;
import java.util.List;

public class CubeDescriptor {
	
	String sourceTable;
	
	String cubeName;
	
	String cubeDataTable;
	
	String cubeIndexTable;
	
	List<CubeMeasure> measures = new ArrayList<CubeMeasure>();
	
	List<CubeDimension> dimensions = new ArrayList<CubeDimension>();

	public List<CubeMeasure> getMeasures() {
		return measures;
	}

	public void setMeasures(List<CubeMeasure> measures) {
		this.measures = measures;
	}

	public List<CubeDimension> getDimensions() {
		return dimensions;
	}

	public void setDimensions(List<CubeDimension> dimensions) {
		this.dimensions = dimensions;
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTableName) {
		this.sourceTable = sourceTableName;
	}
	
	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
		this.cubeDataTable = cubeName + EngineConstants.DATA_CUBE_NAME_SUFFIX;
		this.cubeIndexTable = cubeDataTable + EngineConstants.CUBE_INDEX_SUFFIX;
	}

	public String getCubeDataTable() {
		return cubeDataTable;
	}

	public String getCubeIndexTable() {
		return cubeIndexTable;
	}
}
