package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] characterAndString = line.split(":",2);
        String character = characterAndString[0];
        String words[] = characterAndString[1].split(" ");

        for (String s : words) {
            word.set(character + ":" + s.toLowerCase());
            context.write(word, one);
        }
    }
}
