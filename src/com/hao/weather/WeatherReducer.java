package com.hao.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {

		//找最大值
		/*
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
		*/
		
		//找最小值
		int minValue = Integer.MAX_VALUE;
		for(IntWritable value : values){
			minValue = Math.min(minValue, value.get());
		}
		context.write(key, new IntWritable(minValue));
		
	}

}
