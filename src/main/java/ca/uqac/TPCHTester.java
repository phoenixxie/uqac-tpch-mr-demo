package ca.uqac;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class TPCHTester extends Configured implements Tool {

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
	public static class Map extends
			Mapper<WritableComparable, HCatRecord, IntWritable, DoubleWritable> {
		public static final Log l4j = LogFactory.getLog(Map.class);
		public static MemoryMXBean memoryMXBean;

		private boolean isLogInfoEnabled = false;

		private long numRows = 0;
		private long nextCntr = 1;

		DateFormat df;
		Date shipDate1;
		Date shipDate2;

		static IntWritable retKey = new IntWritable(1);

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			memoryMXBean = ManagementFactory.getMemoryMXBean();
			l4j.info("maximum memory = "
					+ memoryMXBean.getHeapMemoryUsage().getMax());

			isLogInfoEnabled = l4j.isInfoEnabled();

			df = new SimpleDateFormat("yyyy-MM-dd");
			try {
				shipDate1 = df.parse("1994-01-01");
				shipDate2 = df.parse("1995-01-01");
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void map(
				WritableComparable key,
				HCatRecord value,
				org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {

			if (isLogInfoEnabled) {
				numRows++;
				if (numRows == nextCntr) {
					long used_memory = memoryMXBean.getHeapMemoryUsage()
							.getUsed();
					l4j.info("TPCHTester.Map: processing " + numRows
							+ " rows: used memory = " + used_memory);
					nextCntr = getNextCntr(numRows);
				}
			}

			HCatSchema schema = HCatBaseInputFormat.getTableSchema(context
					.getConfiguration());

			String dateStr = value.getString("l_shipdate", schema);
			Double extendedPrice = value.getDouble("l_extendedprice", schema);
			Double discount = value.getDouble("l_discount", schema);
			Double quantity = value.getDouble("l_quantity", schema);

			if (dateStr == null || extendedPrice == null || discount == null
					|| quantity == null) {
				return;
			}

			try {
				Date shipdate = df.parse(dateStr);
				if (shipdate.before(shipDate1) || shipdate.after(shipDate2)) {
					return;
				}
			} catch (ParseException e) {
				e.printStackTrace();
				return;
			}

			if (discount < 0.05D || discount > 0.07D) {
				return;
			}
			if (quantity >= 24) {
				return;
			}

			context.write(retKey, new DoubleWritable(discount * extendedPrice));
		}

		private long getNextCntr(long cntr) {
			if (cntr >= 1000000) {
				return cntr + 1000000;
			}

			return (cntr << 1);
		}
	}

	public static class Combiner extends
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			Double sum = 0D;
			Iterator<DoubleWritable> itr = values.iterator();
			while (itr.hasNext()) {
				DoubleWritable nxt = itr.next();
				sum += nxt.get();
			}

			context.write(new IntWritable(1), new DoubleWritable(sum));
		}

	}

	public static class Combine extends
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		public static final Log l4j = LogFactory.getLog(Reduce.class);
		public static MemoryMXBean memoryMXBean;
		private boolean isLogInfoEnabled = false;

		static IntWritable retKey = new IntWritable(1);

		private long numRows = 0;
		private long nextCntr = 1;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			memoryMXBean = ManagementFactory.getMemoryMXBean();
			l4j.info("maximum memory = "
					+ memoryMXBean.getHeapMemoryUsage().getMax());

			isLogInfoEnabled = l4j.isInfoEnabled();
		}

		@Override
		protected void reduce(IntWritable key,
				java.lang.Iterable<DoubleWritable> values,
				Combine.Context context) throws IOException,
				InterruptedException {

			Iterator<DoubleWritable> iter = values.iterator();
			Double sum = 0D;
			while (iter.hasNext()) {
				DoubleWritable nxt = iter.next();
				sum += nxt.get();

				if (isLogInfoEnabled) {
					numRows++;
					if (numRows == nextCntr) {
						long used_memory = memoryMXBean.getHeapMemoryUsage()
								.getUsed();
						l4j.info("TPCHTester.Combine: processing " + numRows
								+ " rows: used memory = " + used_memory);
						nextCntr = getNextCntr(numRows);
					}
				}
			}

			context.write(retKey, new DoubleWritable(sum));
		}

		private long getNextCntr(long cntr) {
			if (cntr >= 1000000) {
				return cntr + 1000000;
			}

			return (cntr << 1);
		}
	}

	public static class Reduce
			extends
			Reducer<IntWritable, DoubleWritable, WritableComparable, HCatRecord> {

		public static final Log l4j = LogFactory.getLog(Reduce.class);
		public static MemoryMXBean memoryMXBean;
		private boolean isLogInfoEnabled = false;

		static IntWritable retKey = new IntWritable(1);

		private long numRows = 0;
		private long nextCntr = 1;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			memoryMXBean = ManagementFactory.getMemoryMXBean();
			l4j.info("maximum memory = "
					+ memoryMXBean.getHeapMemoryUsage().getMax());

			isLogInfoEnabled = l4j.isInfoEnabled();
		}

		@Override
		protected void reduce(IntWritable key,
				java.lang.Iterable<DoubleWritable> values,
				Reduce.Context context) throws IOException,
				InterruptedException {

			Iterator<DoubleWritable> iter = values.iterator();
			Double sum = 0D;
			while (iter.hasNext()) {
				DoubleWritable nxt = iter.next();
				sum += nxt.get();

				if (isLogInfoEnabled) {
					numRows++;
					if (numRows == nextCntr) {
						long used_memory = memoryMXBean.getHeapMemoryUsage()
								.getUsed();
						l4j.info("TPCHTester.Reduce: processing " + numRows
								+ " rows: used memory = " + used_memory);
						nextCntr = getNextCntr(numRows);
					}
				}
			}
			HCatRecord record = new DefaultHCatRecord(2);
			record.set(0, sum);

			context.write(retKey, record);
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

		String inputTableName = "lineitem_partition";
		String outputTableName = "q6_forecast_revenue_change";
		String dbName = "default";

		Job job = Job.getInstance(conf, "TPCH");
		job.getConfiguration().setInt(
				HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, 3);
		HCatInputFormat.setInput(job, dbName, inputTableName, null);
		job.setInputFormatClass(HCatInputFormat.class);

		job.setJarByClass(TPCHTester.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);

		OutputJobInfo outinfo = OutputJobInfo.create(dbName, outputTableName,
				null);
		HCatOutputFormat.setOutput(job, outinfo);

		HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
		System.err.println("INFO: output schema explicitly set for writing:"
				+ s);
		HCatOutputFormat.setSchema(job, s);

		job.setOutputFormatClass(HCatOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TPCHTester(), args);
		System.exit(exitCode);
	}
}
