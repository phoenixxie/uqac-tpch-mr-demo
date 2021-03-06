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

public class TPCHQ12 extends Configured implements Tool {
	static final String SEPERATOR = "|";

	// select
	// l_shipmode,
	// sum(case
	// when o_orderpriority ='1-URGENT'
	// or o_orderpriority ='2-HIGH'
	// then 1
	// else 0
	// end
	// ) as high_line_count,
	// sum(case
	// when o_orderpriority <> '1-URGENT'
	// and o_orderpriority <> '2-HIGH'
	// then 1
	// else 0
	// end
	// ) as low_line_count
	// from
	// orders o join lineitem l
	// on
	// o.o_orderkey = l.l_orderkey and l.l_commitdate < l.l_receiptdate
	// and l.l_shipdate < l.l_commitdate and l.l_receiptdate >= '1994-01-01'
	// and l.l_receiptdate < '1995-01-01'
	// where
	// l.l_shipmode = 'MAIL' or l.l_shipmode = 'SHIP'
	// group by l_shipmode
	// order by l_shipmode;
	public static class JoinMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		public static final Log l4j = LogFactory.getLog(JoinMapper.class);
		public static MemoryMXBean memoryMXBean;

		static final int L_ORDERKEY = 0, L_SHIPDATE = 10, L_COMMITDATE = 11,
				L_RECEIPTDATE = 12, L_SHIPMODE = 14;

		static final int O_ORDERKEY = 0, O_ORDERPRIORITY = 5;

		private boolean isLogInfoEnabled = false;

		private long numRows = 0;
		private long nextCntr = 1;

		private Splitter splitter;

		private Text dataText = new Text();
		private boolean isLineItem;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			memoryMXBean = ManagementFactory.getMemoryMXBean();
			l4j.info("maximum memory = "
					+ memoryMXBean.getHeapMemoryUsage().getMax());
			isLogInfoEnabled = l4j.isInfoEnabled();

			splitter = Splitter.on(SEPERATOR).trimResults();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();

			l4j.info("Parsing " + filename);
			isLineItem = filename.equals("lineitem.tbl");
		}

		@Override
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {

			if (isLogInfoEnabled) {
				numRows++;
				if (numRows == nextCntr) {
					long used_memory = memoryMXBean.getHeapMemoryUsage()
							.getUsed();
					l4j.info("TPCHQ12.JoinMapper: processing " + numRows
							+ " rows: used memory = " + used_memory);
					nextCntr = getNextCntr(numRows);
				}
			}

			List<String> values = Lists.newArrayList(splitter.split(value
					.toString()));

			int keyInt;
			if (isLineItem) {
				String l_orderkey = values.get(L_ORDERKEY);
				String l_shipdate = values.get(L_SHIPDATE);
				String l_receiptdate = values.get(L_RECEIPTDATE);
				String l_commitdate = values.get(L_COMMITDATE);
				String l_shipmode = values.get(L_SHIPMODE);

				if (!l_shipmode.equals("MAIL") && !l_shipmode.equals("SHIP")) {
					return;
				}
				if (l_commitdate.compareTo(l_receiptdate) >= 0) {
					return;
				}
				if (l_shipdate.compareTo(l_commitdate) >= 0) {
					return;
				}
				if (l_receiptdate.compareTo("1994-01-01") < 0) {
					return;
				}
				if (l_receiptdate.compareTo("1995-01-01") >= 0) {
					return;
				}

				keyInt = Integer.parseInt(l_orderkey);
				dataText.set("L|" + l_shipmode);
			} else {
				String o_orderkey = values.get(O_ORDERKEY);
				String o_orderpriority = values.get(O_ORDERPRIORITY);

				keyInt = Integer.parseInt(o_orderkey);
				dataText.set("O|" + o_orderpriority);
			}

			context.write(new IntWritable(keyInt), dataText);
		}

		private long getNextCntr(long cntr) {
			if (cntr >= 1000000) {
				return cntr + 1000000;
			}

			return (cntr << 1);
		}
	}

	public static class JoinReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public static final Log l4j = LogFactory.getLog(JoinReducer.class);
		public static MemoryMXBean memoryMXBean;
		private boolean isLogInfoEnabled = false;

		private long numRows = 0;
		private long nextCntr = 1;

		private Splitter splitter;
		private Text dataText = new Text();
		private List<String> shipmodes = new ArrayList<String>();

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
		protected void reduce(IntWritable key, java.lang.Iterable<Text> values,
				JoinReducer.Context context) throws IOException,
				InterruptedException {

			if (isLogInfoEnabled) {
				numRows++;
				if (numRows == nextCntr) {
					long used_memory = memoryMXBean.getHeapMemoryUsage()
							.getUsed();
					l4j.info("TPCHQ12.JoinReducer: processing " + numRows
							+ " rows: used memory = " + used_memory);
					nextCntr = getNextCntr(numRows);
				}
			}
			
			shipmodes.clear();
			String o_orderpriority = null;
			
			for (Text value : values) {
				List<String> parts = Lists.newArrayList(splitter.split(value
						.toString()));

				if (parts.get(0).equals("O")) {
					o_orderpriority = parts.get(1);
				} else {
					shipmodes.add(parts.get(1));
				}
			}

			if (o_orderpriority == null) {
				l4j.error("TPCHQ12.Reduce: cannot find key " + key
						+ " from Order");
				return;
			}

			for (String shipmode : shipmodes) {
				dataText.set(o_orderpriority + "|" + shipmode);
				context.write(key, dataText);
			}

		}

		private long getNextCntr(long cntr) {
			if (cntr >= 1000000) {
				return cntr + 1000000;
			}

			return (cntr << 1);
		}
	}

	public static class GroupMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public static final Log l4j = LogFactory.getLog(GroupMapper.class);
		public static MemoryMXBean memoryMXBean;

		static final int ORDERKEY = 0, ORDERPRIORITY = 1, SHIPMODE = 2;

		private boolean isLogInfoEnabled = false;

		private long numRows = 0;
		private long nextCntr = 1;

		private Splitter splitter;

		private Text keyText = new Text();
		private Text dataText = new Text();

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
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			if (isLogInfoEnabled) {
				numRows++;
				if (numRows == nextCntr) {
					long used_memory = memoryMXBean.getHeapMemoryUsage()
							.getUsed();
					l4j.info("TPCHQ12.GroupMapper: processing " + numRows
							+ " rows: used memory = " + used_memory);
					nextCntr = getNextCntr(numRows);
				}
			}

			List<String> values = Lists.newArrayList(splitter.split(value
					.toString()));

			// String orderkey = values.get(ORDERKEY);
			String orderpriority = values.get(ORDERPRIORITY);
			String shipmode = values.get(SHIPMODE);

			keyText.set(shipmode);
			dataText.set(orderpriority);

			context.write(keyText, dataText);
		}

		private long getNextCntr(long cntr) {
			if (cntr >= 1000000) {
				return cntr + 1000000;
			}

			return (cntr << 1);
		}
	}

	public static class GroupReducer extends Reducer<Text, Text, Text, Text> {

		public static final Log l4j = LogFactory.getLog(GroupReducer.class);
		public static MemoryMXBean memoryMXBean;
		private boolean isLogInfoEnabled = false;

		private long numRows = 0;
		private long nextCntr = 1;

		private Text dataText = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			memoryMXBean = ManagementFactory.getMemoryMXBean();
			l4j.info("maximum memory = "
					+ memoryMXBean.getHeapMemoryUsage().getMax());

			isLogInfoEnabled = l4j.isInfoEnabled();
		}

		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values,
				GroupReducer.Context context) throws IOException,
				InterruptedException {

			int high = 0;
			int low = 0;

			for (Text value : values) {
				if (isLogInfoEnabled) {
					numRows++;
					if (numRows == nextCntr) {
						long used_memory = memoryMXBean.getHeapMemoryUsage()
								.getUsed();
						l4j.info("TPCHQ12.GroupReducer: processing " + numRows
								+ " rows: used memory = " + used_memory);
						nextCntr = getNextCntr(numRows);
					}
				}

				String v = value.toString();

				if (v.equals("1-URGENT") || v.equals("2-HIGH")) {
					++high;
				} else {
					++low;
				}
			}
			dataText.set(high + "|" + low);
			context.write(key, dataText);
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

		String infiles = "/tpch/lineitem/lineitem.tbl,/tpch/orders/orders.tbl";
		String tempfile = "/tmp/q12-temp";
		String outfile = "/tmp/q12";
		Path tempPath = new Path(tempfile);

		Job joinJob = Job.getInstance(conf, "TPCHQ12Join");
		joinJob.setJarByClass(TPCHQ12.class);

		FileInputFormat.addInputPaths(joinJob, infiles);
		FileOutputFormat.setOutputPath(joinJob, tempPath);

		joinJob.setMapperClass(JoinMapper.class);
		joinJob.setReducerClass(JoinReducer.class);
		joinJob.setOutputKeyClass(IntWritable.class);
		joinJob.setOutputValueClass(Text.class);

		if (!joinJob.waitForCompletion(true)) {
			return 1;
		}

		Job groupJob = Job.getInstance(conf, "TPCHQ12Group");
		groupJob.setJarByClass(TPCHQ12.class);

		FileInputFormat.addInputPath(groupJob, tempPath);
		FileOutputFormat.setOutputPath(groupJob, new Path(outfile));

		groupJob.setMapperClass(GroupMapper.class);
		groupJob.setReducerClass(GroupReducer.class);
		groupJob.setOutputKeyClass(Text.class);
		groupJob.setOutputValueClass(Text.class);

		return groupJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TPCHQ12(), args);
		System.exit(exitCode);
	}
}
