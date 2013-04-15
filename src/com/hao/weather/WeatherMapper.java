package com.hao.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(14, 20);
		int airTemperature;
		if (line.charAt(26) == '+') {
			airTemperature = Integer.parseInt(line.substring(27, 32));
		} else {
			airTemperature = Integer.parseInt(line.substring(26, 32));
		}
		context.write(new Text(year), new IntWritable(airTemperature));
	}

}
