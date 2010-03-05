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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.olap4cloud.util.BytesPackUtils;

public class GenerateCube {
	
	public static final String JOB_CONF_PROP_DIMENSIONS = "JOB_CONF_PROP_DIMENSIONS";
	
	public static final String JOB_CONF_PROP_MEASURES = "JOB_CONF_PROP_MEASURES";
	
	public static final String JOB_CONF_PROP_FAMILY = "JOB_CONF_PROP_FAMILY";
	
	public static final String DATA_CUBE_NAME_PREFIX = "_data_cube";
	
	public static class GenerateCubeMapper extends TableMapper<ImmutableBytesWritable, Put>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			String dimensionColumns[] = context.getConfiguration().getStrings(JOB_CONF_PROP_DIMENSIONS);
			String measuresColumns[] = context.getConfiguration().getStrings(JOB_CONF_PROP_MEASURES);
			String family = context.getConfiguration().getStrings(JOB_CONF_PROP_FAMILY)[0];
			byte familyBytes[] = Bytes.toBytes(family);
			long dimensions[] = new long[dimensionColumns.length + 1];
			for(int i = 0; i < dimensions.length - 1; i ++) 
				dimensions[i] = Bytes.toLong(value.getValue(familyBytes, Bytes.toBytes(dimensionColumns[i])));
			dimensions[dimensions.length - 1] = Bytes.toLong(value.getRow());
			byte cubeKey[] = BytesPackUtils.pack(dimensions);
			Put put = new Put(cubeKey);
			for(int i = 0; i < measuresColumns.length; i ++) {
				byte column[] = Bytes.toBytes(measuresColumns[i]);
				byte columnFamily[] = Bytes.toBytes(measuresColumns[i] + "f");
				double measure = Bytes.toDouble(value.getValue(familyBytes, column));
				put.add(columnFamily, column, Bytes.toBytes(measure));
			}
			context.write(new ImmutableBytesWritable(cubeKey), put);
		}
	}
	
	public static void generateCube(String tableName, String family, String dimensions, String measures) 
		throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor(tableName + DATA_CUBE_NAME_PREFIX);
		String measuresFamilies[] = getMeasuresFamilies(measures);
		for(int i = 0; i < measuresFamilies.length; i ++)
			tableDescr.addFamily(new HColumnDescriptor(Bytes.toBytes(measuresFamilies[i])));
		admin.createTable(tableDescr);
		Job job = new Job();
		job.setJarByClass(GenerateCube.class);
		TableMapReduceUtil.initTableMapperJob(tableName, new Scan(), GenerateCubeMapper.class
				, ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName + DATA_CUBE_NAME_PREFIX
				, IdentityTableReducer.class, job);
		job.getConfiguration().set(JOB_CONF_PROP_FAMILY, family);
		job.getConfiguration().set(JOB_CONF_PROP_DIMENSIONS, dimensions);
		job.getConfiguration().set(JOB_CONF_PROP_MEASURES, measures);
		job.waitForCompletion(true);
	}
	
	private static String[] getMeasuresFamilies(String measures) {
		return measures.split(",");
	}

	public static void main(String argv[]) throws Exception {
		generateCube("testfacttable", "data", "d1,d2,d3", "m1,m2,m3");
	}
}
