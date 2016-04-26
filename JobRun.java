package com.cn.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "host1:9001");
		Job job;
		try {
			job = new Job(conf);
			job.setJarByClass(JobRun.class);
			job.setJobName("wordcount");

			job.setMapperClass(WcMapper.class);
			job.setReducerClass(WcReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path("/user/root/wc/input"));
			FileOutputFormat.setOutputPath(job,
					new Path("/user/root/wc/output"));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
