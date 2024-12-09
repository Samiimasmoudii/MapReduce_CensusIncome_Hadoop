package org.example.Hours;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaritalStatusMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable hoursWorked = new IntWritable();
    private Text maritalStatus = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 12) {
            maritalStatus.set(fields[5].trim());  // Assuming marital-status is the 6th field
            hoursWorked.set(Integer.parseInt(fields[12].trim()));  // Assuming hours-per-week is the 13th field
            context.write(maritalStatus, hoursWorked);
        }
    }
}
