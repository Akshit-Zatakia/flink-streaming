package org.akshit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Create a stream from a socket source (Replace 'hostname' and 'port' with your source)
		DataStream<String> text = env.socketTextStream("localhost", 9999);

		// Transform the stream: split the lines into words, count, and print
		DataStream<Tuple2<String, Integer>> counts = text
				.flatMap(new Tokenizer())
				.keyBy(0)
				.sum(1);

		counts.print();

		// Execute program, beginning computation.
		env.execute("Word count streaming");
	}

	public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W+");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
