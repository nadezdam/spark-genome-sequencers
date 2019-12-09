package sequence.input.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class SequenceRecordReader extends RecordReader<LongWritable,Text> {
    private LineRecordReader lineRecordReader;
    private String prevSequence;
    private long offset;
    private LongWritable key;
    private Text value;
    private int patternLength;

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }

    @Override
    public LongWritable getCurrentKey() throws
            InterruptedException {

        return this.key;
    }

    @Override
    public Text getCurrentValue() {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException {
        this.lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(inputSplit, context);
        Configuration conf = context.getConfiguration();
        this.patternLength = Integer.parseInt(conf.get("patternLength"));
        this.key = new LongWritable();
        this.value = new Text();
        if (lineRecordReader.nextKeyValue()) {
            this.prevSequence = lineRecordReader.getCurrentValue().toString();
            this.offset = lineRecordReader.getCurrentKey().get();
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException {

        if (prevSequence == null) {
            this.value = null;
            this.key = null;
            return false;
        }
        String currentSequence = prevSequence;
        this.key.set(offset);
        this.prevSequence = null;

        if (lineRecordReader.nextKeyValue()) {
            this.prevSequence = lineRecordReader.getCurrentValue().toString();
            currentSequence += prevSequence
                    .substring(0, patternLength - 1);

            this.offset = lineRecordReader.getCurrentKey().get();

        }

        this.value.set(currentSequence);
        return true;
    }
}

