package org.lectures;

import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Weird hack to take a filename of a file in HDFS and return that
 * name as the 1 and only 1 record "read" from it.
 */
public class FilenameInputFormat extends FileInputFormat<Text, Text> {
	/**
	 * By definition, not splitable.
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	/**
	 * Return a RecordReader which returns 1 record: the file path from
	 * the InputSplit.
	 */
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		context.setStatus(genericSplit.toString());

		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();

		return new RecordReader<Text, Text>() {
			private Text key, value;
			boolean done = false;

			@Override
			public void close() {
			}

			@Override
			public Text getCurrentKey() {
				return key;
			}

			@Override
			public Text getCurrentValue() {
				return value;
			}

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context) {
				key = new Text();
				value = new Text();
			}

			@Override
			public float getProgress() {
				return 0.0f;
			}

			@Override
			public boolean nextKeyValue() {
				if (done)
					return false;

				key.set(file.toString());
				value.set(file.toString());

				done = true;

				return true;
			}

		};
	}

}
