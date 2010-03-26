package org.olap4cloud.client;

public class CubeDimension {
	String sourceField;
	
	String name;
	
	public CubeDimension(String sourceField, String name) {
		this.sourceField = sourceField;
		this.name = name;
	}
	
	public String getSourceField() {
		return sourceField;
	}

	public void setSourceField(String sourceField) {
		this.sourceField = sourceField;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
