package org.olap4cloud.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.olap4cloud.impl.OLAPEngineConstants;

public class CubeDescriptor implements Serializable {
	
	String cubeName;
	
	String cubeDataTable;
	
	String cubeIndexTable;
	
	String sourceDataDir;
	
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
	
	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
		this.cubeDataTable = cubeName + OLAPEngineConstants.DATA_CUBE_NAME_SUFFIX;
		this.cubeIndexTable = cubeDataTable + OLAPEngineConstants.CUBE_INDEX_SUFFIX;
	}

	public String getCubeDataTable() {
		return cubeDataTable;
	}

	public String getCubeIndexTable() {
		return cubeIndexTable;
	}
	
	public String getSourceDataDir() {
		return sourceDataDir;
	}

	public void setSourceDataDir(String sourceDataDir) {
		this.sourceDataDir = sourceDataDir;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Cube[name = ")
			.append(cubeName)
			.append(" measures = {");
		for(CubeMeasure measure: measures)
			sb.append("(name = ").append(measure.getName()).append(", sourceField = ").append(measure.getSourceField())
				.append(") ");
		return sb.toString();
	}
}
