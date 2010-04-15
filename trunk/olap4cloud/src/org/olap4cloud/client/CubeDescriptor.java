package org.olap4cloud.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.olap4cloud.impl.OLAPEngineConstants;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class CubeDescriptor implements Serializable {
	
	static Logger logger = Logger.getLogger(CubeDescriptor.class);
	
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
			sb.append("(name = ").append(measure.getName()).append(") ");
		return sb.toString();
	}
	
	public void loadFromClassPath(String resourceName, ClassLoader classLoader) throws OLAPEngineException {
		InputStream in = null;
		try {
			in = classLoader.getResourceAsStream(resourceName);
			if(in == null)
				throw new OLAPEngineException("Can't find " + resourceName + " in classpath.");
			load(in);
		} finally {
			try {
				if(in != null)
					in.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
				throw new OLAPEngineException(e);
			}
		}
	}
	
	public void load(InputStream in) throws OLAPEngineException {
		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory
					.newInstance();
			documentBuilderFactory.setIgnoringComments(true);
			documentBuilderFactory.setNamespaceAware(true);
			try {
				documentBuilderFactory.setXIncludeAware(true);
			} catch (UnsupportedOperationException e) {
				logger.error("Failed to set setXIncludeAware(true) for parser "
						+ documentBuilderFactory + ":" + e, e);
			}
			DocumentBuilder documentBuilder = documentBuilderFactory
					.newDocumentBuilder();
			Document document = documentBuilder.parse(in);
			Element root = document.getDocumentElement();
			if(!"cube".equalsIgnoreCase(root.getTagName())) 
				throw new OLAPEngineException("Bad configuration file: top-level element is not <cube>");
			String cubeName = root.getAttribute("name");
			if(cubeName == null)
				throw new OLAPEngineException("Bad configuration file: <cube> element does not have 'name' attribute");
			setCubeName(cubeName);
			String sourcePath = root.getAttribute("sourcePath");
			if(sourcePath == null)
				throw new OLAPEngineException("Bad configuration file: <cube> element does not have 'sourcePath' " +
						"attribute");
			setSourceDataDir(sourcePath);
			NodeList dimensions = root.getElementsByTagName("dimension");
			for(int i = 0; i < dimensions.getLength(); i ++) {
				Element dimension = (Element)dimensions.item(i);
				String name = dimension.getAttribute("name");
				if(name == null)
					throw new OLAPEngineException("Bad configuration file: <dimension> element does not have " +
							"'name' attribute.");
				getDimensions().add(new CubeDimension(name));
			}
			NodeList measures = root.getElementsByTagName("measure");
			for(int i = 0; i < measures.getLength(); i ++) {
				Element measure = (Element)measures.item(i);
				String name = measure.getAttribute("name");
				if(name == null)
					throw new OLAPEngineException("Bad configuration file: <measure> element does not have " +
							"'name' attribute.");
				getMeasures().add(new CubeMeasure(name));
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new OLAPEngineException(e);
		}
	}
}
