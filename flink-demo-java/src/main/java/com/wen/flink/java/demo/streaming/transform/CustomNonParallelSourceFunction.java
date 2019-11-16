package com.wen.flink.java.demo.streaming.transform;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomNonParallelSourceFunction implements SourceFunction<Long> {
    boolean isRunning=true;
    long count=1;
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning){
            ctx.collect(count);
            count+=1;
            if(count>5){
                cancel();
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
