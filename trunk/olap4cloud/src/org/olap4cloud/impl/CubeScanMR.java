package org.olap4cloud.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeDimension;
import org.olap4cloud.client.CubeMeasure;
import org.olap4cloud.impl.GenerateCubeIndexMR.GenerateCubeIndexMapper;
import org.olap4cloud.util.LogUtils;


public class CubeScanMR {
	
	static Logger logger = Logger.getLogger(CubeScanMR.class);
	
	public static class CubeScanMRMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			double m = Bytes.toDouble(value.getValue(Bytes.toBytes(EngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX + "m1")
					, Bytes.toBytes("m1")));
			context.write(new ImmutableBytesWritable(new byte[]{1}), new DoubleWritable(m));
		}
	}
	
	public static class CubeScanMRReducer extends Reducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable
		, DoubleWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable inKey,
				Iterable<DoubleWritable> inVal,
				org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable, DoubleWritable
				, ImmutableBytesWritable
				, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double s = 0;
			for(Iterator<DoubleWritable> i = inVal.iterator(); i.hasNext(); ) {
				s += i.next().get();
			}
			context.write(new ImmutableBytesWritable(new byte[]{1}), new DoubleWritable(s));
		}
	}
	
	public static void scan(CubeDescriptor descr) throws Exception {
		Job job = new Job();
		job.setJarByClass(CubeScanMR.class);
		List<Scan> scans = getScans(descr);
		TableMapReduceUtil.initTableMapperJob(descr.getCubeDataTable(), scans.get(0), CubeScanMRMapper.class
				, ImmutableBytesWritable.class, DoubleWritable.class, job);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(CubeScanMRReducer.class);
		job.setCombinerClass(CubeScanMRReducer.class);
		FileOutputFormat.setOutputPath(job, new Path("/out"));
		job.waitForCompletion(true);
	}

	private static List<Scan> getScans(CubeDescriptor descr) throws Exception {
		String methodName = "getScans() ";
		List<Scan> scans = new ArrayList<Scan>();
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bout);
		out.writeLong(1);
		for(int i = 0; i < descr.getDimensions().size(); i ++)
			out.writeLong(0);
		out.close();
		bout.close();
		byte startRow[] = bout.toByteArray();
		bout = new ByteArrayOutputStream();
		out = new DataOutputStream(bout);
		out.writeLong(1);
		for(int i = 0; i < descr.getDimensions().size(); i ++)
			out.writeLong(Long.MIN_VALUE);
		out.close();
		bout.close();
		byte stopRow[] = bout.toByteArray();
		logger.debug(methodName + " generate scan for " + LogUtils.describe(startRow) + " start row and " 
				+ LogUtils.describe(stopRow) + " stop row");
		Scan scan = new Scan();
		scan.setStartRow(startRow);
		scan.setStopRow(stopRow);
		scan.addColumn(Bytes.toBytes("family_m1"), Bytes.toBytes("m1"));
		scans.add(scan);
		return scans;
	}
	
	public static void main(String argv[]) throws Exception {
		CubeDescriptor descr = new CubeDescriptor();
		descr.setSourceTable("testfacttable");
		descr.setCubeName("testcube");
		descr.getDimensions().add(new CubeDimension("data.d1", "d1"));
		descr.getDimensions().add(new CubeDimension("data.d2", "d2"));
		descr.getDimensions().add(new CubeDimension("data.d3", "d3"));
		descr.getMeasures().add(new CubeMeasure("data.m1", "m1"));
		descr.getMeasures().add(new CubeMeasure("data.m2", "m2"));
		descr.getMeasures().add(new CubeMeasure("data.m3", "m3"));
		scan(descr);
	}
}
