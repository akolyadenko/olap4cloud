package org.olap4cloud;

import java.util.ArrayList;
import java.util.List;

public class CubeDescriptor {
	
	String sourceTable;
	
	String cubeName;
	
	String cubeDataTable;
	
	String cubeIndexTable;
	
	List<String> measures = new ArrayList<String>();
	
	List<String> sourceDimensions = new ArrayList<String>();

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTableName) {
		this.sourceTable = sourceTableName;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public void setMeasures(List<String> measures) {
		this.measures = measures;
	}

	public List<String> getSourceDimensions() {
		return sourceDimensions;
	}

	public void setSourceDimensions(List<String> sourceDimensions) {
		this.sourceDimensions = sourceDimensions;
	}

	public String getCubeName() {
		return cubeName;
	}
	
	public String getMeasuresAsString() {
		StringBuilder sb = new StringBuilder("");
		if(measures.size() > 0)
			sb.append(measures.get(0));
		for(int i = 1; i < measures.size(); i ++)
			sb.append(",").append(measures.get(i));
		return sb.toString();
	}
	
	public String getDimensionsAsString() {
		StringBuilder sb = new StringBuilder("");
		if(sourceDimensions.size() > 0)
			sb.append(sourceDimensions.get(0));
		for(int i = 1; i < sourceDimensions.size(); i ++)
			sb.append(",").append(sourceDimensions.get(i));
		return sb.toString();
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
