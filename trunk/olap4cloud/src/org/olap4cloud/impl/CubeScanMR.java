package org.olap4cloud.impl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQueryResult;
import org.olap4cloud.test.TestCubeScanMR.TestCubeScanMRMapper;
import org.olap4cloud.test.TestCubeScanMR.TestCubeScanMRReducer;

public class CubeScanMR {
	
	public static CubeQueryResult scan(CubeScan scan, CubeDescriptor cubeDescriptor) throws Exception {
		Job job = new Job();
		job.setJarByClass(CubeScanMR.class);
		TableMapReduceUtil.initTableMapperJob(cubeDescriptor.getCubeDataTable(), scan.getHBaseScan()
				, CubeScanMRMapper.class
				, ImmutableBytesWritable.class, DoubleWritable.class, job);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(CubeScanMRReducer.class);
		job.setCombinerClass(CubeScanMRReducer.class);
		String outPath = "/olap4cloud/out" + job.getJobID();
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.waitForCompletion(true);
		return null;
	}
	
	
	
	public static class CubeScanMRMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			
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
			
		}
		
	}
}
