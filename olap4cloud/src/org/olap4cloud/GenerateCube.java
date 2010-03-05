package org.olap4cloud;

import java.io.IOException;

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
import org.olap4cloud.util.BytesPackUtils;

public class GenerateCube {
	
	public static class GenerateCubeMapper extends TableMapper<ImmutableBytesWritable, Put>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			String dimensionColumns[] = context.getConfiguration().getStrings(EngineConstants.JOB_CONF_PROP_DIMENSIONS);
			String measuresColumns[] = context.getConfiguration().getStrings(EngineConstants.JOB_CONF_PROP_MEASURES);
			long dimensions[] = new long[dimensionColumns.length + 1];
			for(int i = 0; i < dimensions.length - 1; i ++) {
				String splits[] = dimensionColumns[i].split("\\.");
				byte family[] = Bytes.toBytes(splits[0]);
				byte column[] = Bytes.toBytes(splits[1]);
				dimensions[i] = Bytes.toLong(value.getValue(family, column));
			}
			dimensions[dimensions.length - 1] = Bytes.toLong(value.getRow());
			byte cubeKey[] = BytesPackUtils.pack(dimensions);
			Put put = new Put(cubeKey);
			for(int i = 0; i < measuresColumns.length; i ++) {
				byte column[] = Bytes.toBytes(measuresColumns[i].split("\\.")[1]);
				byte family[] = Bytes.toBytes(measuresColumns[i].split("\\.")[0]);
				byte columnFamily[] = Bytes.toBytes(EngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX + measuresColumns[i]);
				double measure = Bytes.toDouble(value.getValue(family, column));
				put.add(columnFamily, column, Bytes.toBytes(measure));
			}
			context.write(new ImmutableBytesWritable(cubeKey), put);
		}
	}
	
	public static void generateCube(CubeDescriptor descr)//String tableName, String family, String dimensions, String measures) 
		throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor(descr.getCubeDataTableName());
		String measuresFamilies[] = descr.getMeasures().toArray(new String[0]);
		for(int i = 0; i < measuresFamilies.length; i ++)
			tableDescr.addFamily(new HColumnDescriptor(Bytes.toBytes(EngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX 
					+ measuresFamilies[i])));
		admin.createTable(tableDescr);
		Job job = new Job();
		job.setJarByClass(GenerateCube.class);
		TableMapReduceUtil.initTableMapperJob(descr.getSourceTableName(), new Scan(), GenerateCubeMapper.class
				, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob(descr.getCubeDataTableName()
				, IdentityTableReducer.class, job);
		job.getConfiguration().set(EngineConstants.JOB_CONF_PROP_DIMENSIONS, descr.getDimensionsAsString());
		job.getConfiguration().set(EngineConstants.JOB_CONF_PROP_MEASURES, descr.getMeasuresAsString());
		job.waitForCompletion(true);
	}
	
	public static void main(String argv[]) throws Exception {
		CubeDescriptor descr = new CubeDescriptor();
		descr.setSourceTableName("testfacttable");
		descr.setCubeName("testcube");
		descr.getSourceDimensions().add("data.d1");
		descr.getSourceDimensions().add("data.d2");
		descr.getSourceDimensions().add("data.d3");
		descr.getMeasures().add("data.m1");
		descr.getMeasures().add("data.m2");
		descr.getMeasures().add("data.m3");
	}
}
