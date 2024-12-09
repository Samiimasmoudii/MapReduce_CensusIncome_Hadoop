package org.example.Hours;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReducer
        extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        double average = (double) sum / count;
        result.set(average);
        context.write(key, result); // Writing average as DoubleWritable
    }
}
