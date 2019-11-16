package com.wen.flink.java.demo.streaming.data_source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class CustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {
    boolean isRunning=true;
    long count=1;
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning){
            ctx.collect(count);
            count+=1;
            Thread.sleep(1000);
        }
    }


    @Override
    public void cancel() {
        isRunning=false;
    }
}
