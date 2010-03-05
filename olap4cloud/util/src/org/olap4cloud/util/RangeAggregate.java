package org.olap4cloud.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.olap4cloud.util.DataImportHFile.DataImportMapper;

public class RangeAggregate {
	
	static Logger logger = Logger.getLogger(RangeAggregate.class);
	
	public static class RangeAggregateMapper extends TableMapper<LongWritable, DoubleWritable> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			byte mb1[] = value.getValue(Bytes.toBytes("data"), Bytes.toBytes("m1"));
			double m1 = Bytes.toDouble(value.getValue(Bytes.toBytes("data"), Bytes.toBytes("m1")));
			context.write(new LongWritable(1), new DoubleWritable(m1));
		}
	}
	
	public static class RangeAggregateReducer extends Reducer<LongWritable, DoubleWritable
		, LongWritable, DoubleWritable> {
		@Override
		protected void reduce(LongWritable arg0,
				Iterable<DoubleWritable> it,
				org.apache.hadoop.mapreduce.Reducer<LongWritable, DoubleWritable
				, LongWritable, DoubleWritable>.Context c)
				throws IOException, InterruptedException {
			double s = 0;
			for(Iterator<DoubleWritable> i = it.iterator(); i.hasNext(); ) {
				DoubleWritable d = i.next();
				s ++;
			}
			c.write(new LongWritable(1), new DoubleWritable(s));
		}
	}
	
	public static void main(String argv[]) throws Exception {
		Job job = new Job();
		job.setJarByClass(RangeAggregate.class);
		Scan s = new Scan();
		s.setStartRow(new ImmutableBytesWritable(Bytes.toBytes(5)).get());
		s.setStopRow(new ImmutableBytesWritable(Bytes.toBytes(15)).get());
		TableMapReduceUtil.initTableMapperJob("testfacttable", s, RangeAggregateMapper.class
				, LongWritable.class, DoubleWritable.class, job);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(RangeAggregateReducer.class);
		FileOutputFormat.setOutputPath(job, new Path("/out"));
		job.waitForCompletion(true);
	}
}