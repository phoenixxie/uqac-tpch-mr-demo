package ca.uqac;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class TPCHQ6 extends Configured implements Tool {
	static final String SEPERATOR = "|";

	// query 6th:
	// insert overwrite table q6_forecast_revenue_change
	// select
	// sum(l_extendedprice*l_discount) as revenue
	// from
	// lineitem
	// where
	// l_shipdate >= '1994-01-01'
	// and l_shipdate < '1995-01-01'
	// and l_discount >= 0.05 and l_discount <= 0.07
	// and l_quantity < 24;
	public static class SelectMapper extends
			Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		public static final Log l4j = LogFactory.getLog(SelectMapper.class);
		public static MemoryMXBean memoryMXBean;

		static final int L_QUANTITY = 4, L_EXTENDEDPRICE = 5, L_DISCOUNT = 6,
				L_SHIPDATE = 10;

		private boolean isLogInfoEnabled = false;

		private long numRows = 0;
		private long nextCntr = 1;

		private Splitter splitter;

		private static final IntWritable keyInt = new IntWritable(1);
		private DoubleWritable dataDouble = new DoubleWritable();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			memoryMXBean = ManagementFactory.getMemoryMXBean();
			l4j.info("maximum memory = "
					+ memoryMXBean.getHeapMemoryUsage().getMax());

			splitter = Splitter.on(SEPERATOR).trimResults();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();

			l4j.info("Parsing " + filename);
		}

		@Override
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {

			if (isLogInfoEnabled) {
				numRows++;
				if (numRows == nextCntr) {
					long used_memory = memoryMXBean.getHeapMemoryUsage()
							.getUsed();
					l4j.info("TPCHQ6.SelectMapper: processing " + numRows
							+ " rows: used memory = " + used_memory);
					nextCntr = getNextCntr(numRows);
				}
			}

			List<String> values = Lists.newArrayList(splitter.split(value
					.toString()));

			String l_quantity = values.get(L_QUANTITY);
			String l_discount = values.get(L_DISCOUNT);
			String l_shipdate = values.get(L_SHIPDATE);
			String l_extendedprice = values.get(L_EXTENDEDPRICE);

			double discount = Double.parseDouble(l_discount);
			double quantity = Double.parseDouble(l_quantity);
			double extendedprice = Double.parseDouble(l_extendedprice);

			if (l_shipdate.compareTo("1994-01-01") < 0) {
				return;
			}
			if (l_shipdate.compareTo("1995-01-01") >= 0) {
				return;
			}
			if (discount < 0.05d || discount > 0.07) {
				return;
			}
			if (quantity >= 24d) {
				return;
			}

			dataDouble.set(extendedprice * discount);

			context.write(keyInt, dataDouble);
		}

		private long getNextCntr(long cntr) {
			if (cntr >= 1000000) {
				return cntr + 1000000;
			}

			return (cntr << 1);
		}
	}

	public static class SelectReducer extends
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		public static final Log l4j = LogFactory.getLog(SelectReducer.class);
		public static MemoryMXBean memoryMXBean;
		private boolean isLogInfoEnabled = false;

		private long numRows = 0;
		private long nextCntr = 1;

		private Splitter splitter;
		private static final IntWritable keyInt = new IntWritable(1);
		private DoubleWritable dataDouble = new DoubleWritable();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			memoryMXBean = ManagementFactory.getMemoryMXBean();
			l4j.info("maximum memory = "
					+ memoryMXBean.getHeapMemoryUsage().getMax());

			isLogInfoEnabled = l4j.isInfoEnabled();

			splitter = Splitter.on(SEPERATOR).trimResults();
		}

		@Override
		protected void reduce(IntWritable key,
				java.lang.Iterable<DoubleWritable> values,
				SelectReducer.Context context) throws IOException,
				InterruptedException {

			double sum = 0d;
			for (DoubleWritable value : values) {
				sum += value.get();

				if (isLogInfoEnabled) {
					numRows++;
					if (numRows == nextCntr) {
						long used_memory = memoryMXBean.getHeapMemoryUsage()
								.getUsed();
						l4j.info("TPCHQ6.SelectReducer: processing " + numRows
								+ " rows: used memory = " + used_memory);
						nextCntr = getNextCntr(numRows);
					}
				}

			}

			dataDouble.set(sum);
			context.write(keyInt, dataDouble);
		}

		private long getNextCntr(long cntr) {
			if (cntr >= 1000000) {
				return cntr + 1000000;
			}

			return (cntr << 1);
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapreduce.output.textoutputformat.separator", SEPERATOR);

		String infiles = "/tpch/lineitem/lineitem.tbl";
		String outfile = "/tmp/q6";

		Job selectJob = Job.getInstance(conf, "TPCHQ6Select");
		selectJob.setJarByClass(TPCHQ6.class);

		FileInputFormat.addInputPaths(selectJob, infiles);
		FileOutputFormat.setOutputPath(selectJob, new Path(outfile));

		selectJob.setMapperClass(SelectMapper.class);
		selectJob.setCombinerClass(SelectReducer.class);
		selectJob.setReducerClass(SelectReducer.class);
		selectJob.setOutputKeyClass(Text.class);
		selectJob.setOutputValueClass(Text.class);

		return selectJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TPCHQ6(), args);
		System.exit(exitCode);
	}
}
