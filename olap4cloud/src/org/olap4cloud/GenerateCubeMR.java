package org.olap4cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.olap4cloud.util.BytesPackUtils;

public class GenerateCubeMR {
	
	public static class GenerateCubeMapper extends TableMapper<ImmutableBytesWritable, Put>{
		
		static Logger logger = Logger.getLogger(GenerateCubeMapper.class);
		
		String dimensionColumns[] = null;
		
		String measuresNames[] = null;
		
		String measuresColumns[] = null;
		
		List<String[]> dimensionColumnSplits = new ArrayList<String[]>();
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			dimensionColumns = context.getConfiguration()
				.getStrings(EngineConstants.JOB_CONF_PROP_DIMENSION_SOURCE_FIELDS);
			measuresColumns = context.getConfiguration()
				.getStrings(EngineConstants.JOB_CONF_PROP_MEASURE_SOURCE_FIELDS);
			measuresNames = context.getConfiguration()
				.getStrings(EngineConstants.JOB_CONF_PROP_MEASURES);
			for(int i = 0; i < dimensionColumns.length; i ++)
				dimensionColumnSplits.add(dimensionColumns[i].split("\\."));
		}
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			long dimensions[] = new long[dimensionColumns.length + 1];
			for(int i = 0; i < dimensions.length - 1; i ++) {
//				logger.debug("map() splits[0] = " + dimensionColumnSplits.get(i)[0]);
//				logger.debug("map() splits[1] = " + dimensionColumnSplits.get(i)[1]);
				byte family[] = Bytes.toBytes(dimensionColumnSplits.get(i)[0]);
				byte column[] = Bytes.toBytes(dimensionColumnSplits.get(i)[1]);
				dimensions[i] = Bytes.toLong(value.getValue(family, column));
//				logger.debug("map() dimesnion[" + i + "] = " + dimensions[i]);
			}
			dimensions[dimensions.length - 1] = Bytes.toLong(value.getRow());
			byte cubeKey[] = BytesPackUtils.pack(dimensions);
			Put put = new Put(cubeKey);
			for(int i = 0; i < measuresColumns.length; i ++) {
				byte column[] = Bytes.toBytes(measuresColumns[i].split("\\.")[1]);
				byte family[] = Bytes.toBytes(measuresColumns[i].split("\\.")[0]);
				byte measureName[] = Bytes.toBytes(measuresNames[i]);
				byte measureFamily[] = Bytes.toBytes(EngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX 
						+ measuresNames[i]);
				double measure = Bytes.toDouble(value.getValue(family, column));
				put.add(measureFamily, measureName, Bytes.toBytes(measure));
			}
			context.write(new ImmutableBytesWritable(cubeKey), put);
		}
	}
	
	public static void generateCube(CubeDescriptor descr) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor(descr.getCubeDataTable());
		for(int i = 0; i < descr.getMeasures().size(); i ++)
			tableDescr.addFamily(new HColumnDescriptor(EngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX 
					+ descr.getMeasures().get(i).getName()));
		if(admin.tableExists(descr.getCubeDataTable())) {
			admin.disableTable(descr.getCubeDataTable());
			admin.deleteTable(descr.getCubeDataTable());
		}
		admin.createTable(tableDescr);
		Job job = new Job();
		job.setJarByClass(GenerateCubeMR.class);
		TableMapReduceUtil.initTableMapperJob(descr.getSourceTable(), new Scan(), GenerateCubeMapper.class
				, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob(descr.getCubeDataTable()
				, IdentityTableReducer.class, job);
		job.getConfiguration().set(EngineConstants.JOB_CONF_PROP_DIMENSIONS
				, getDimensionsAsString(descr));
		job.getConfiguration().set(EngineConstants.JOB_CONF_PROP_DIMENSION_SOURCE_FIELDS
				, getDimensionSourceFieldsAsString(descr));
		job.getConfiguration().set(EngineConstants.JOB_CONF_PROP_MEASURES
				, getMeasuresAsString(descr));
		job.getConfiguration().set(EngineConstants.JOB_CONF_PROP_MEASURE_SOURCE_FIELDS
				, getMeasureSourceFields(descr));
		job.waitForCompletion(true);
	}
	
	private static String getMeasureSourceFields(CubeDescriptor descr) {
		StringBuilder sb = new StringBuilder("");
		if(descr.getMeasures().size() > 0)
			sb.append(descr.getMeasures().get(0).getSourceField());
		for(int i = 1; i < descr.getMeasures().size(); i ++)
			sb.append(",").append(descr.getMeasures().get(i).getSourceField());
		return sb.toString();
	}

	private static String getMeasuresAsString(CubeDescriptor descr) {
		StringBuilder sb = new StringBuilder("");
		if(descr.getMeasures().size() > 0)
			sb.append(descr.getMeasures().get(0).getName());
		for(int i = 1; i < descr.getMeasures().size(); i ++)
			sb.append(",").append(descr.getMeasures().get(i).getName());
		return sb.toString();
	}

	private static String getDimensionSourceFieldsAsString(CubeDescriptor descr) {
		StringBuilder sb = new StringBuilder("");
		if(descr.getDimensions().size() > 0)
			sb.append(descr.getDimensions().get(0).getSourceField());
		for(int i = 1; i < descr.getDimensions().size(); i ++)
			sb.append(",").append(descr.getDimensions().get(i).getSourceField());
		return sb.toString();

	}

	private static String getDimensionsAsString(CubeDescriptor descr) {
		StringBuilder sb = new StringBuilder("");
		if(descr.getDimensions().size() > 0)
			sb.append(descr.getDimensions().get(0).getName());
		for(int i = 1; i < descr.getDimensions().size(); i ++)
			sb.append(",").append(descr.getDimensions().get(i).getName());
		return sb.toString();
	}

	public static void main(String argv[]) throws Exception {
		CubeDescriptor descr = createTestCubeDescriptor();
		generateCube(descr);
		GenerateCubeIndexMR.generate(descr);
	}

	public static CubeDescriptor createTestCubeDescriptor() {
		CubeDescriptor descr = new CubeDescriptor();
		descr.setSourceTable("testfacttable");
		descr.setCubeName("testcube");
		descr.getDimensions().add(new CubeDimension("data.d1", "d1"));
		descr.getDimensions().add(new CubeDimension("data.d2", "d2"));
		descr.getDimensions().add(new CubeDimension("data.d3", "d3"));
		descr.getMeasures().add(new CubeMeasure("data.m1", "m1"));
		descr.getMeasures().add(new CubeMeasure("data.m2", "m2"));
		descr.getMeasures().add(new CubeMeasure("data.m3", "m3"));
		return descr;
	}
}
