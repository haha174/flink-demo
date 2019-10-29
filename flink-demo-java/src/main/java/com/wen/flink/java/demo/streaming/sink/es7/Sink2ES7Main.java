package com.wen.flink.java.demo.streaming.sink.es7;


import com.wen.flink.java.demo.entity.MetricEvent;
import com.wen.tools.json.util.JsonObject;
import com.wen.tools.log.utils.LogUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.UnsupportedEncodingException;
import java.util.List;


/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Sink2ES7Main {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("10.148.185.205", 9999);

        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses("http://10.148.185.205:9200");
//        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
//        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);
        int bulkSize = 1;
        int sinkParallelism = 1;
        SingleOutputStreamOperator<MetricEvent> dataMetricEvent = data.map(new MapFunction<String, MetricEvent>() {
            @Override
            public MetricEvent map(String value) throws Exception {
                MetricEvent metricEvent = new MetricEvent();
                metricEvent.setName(value);
                metricEvent.setTimestamp(System.currentTimeMillis());
                return metricEvent;
            }
        });

        LogUtil.getCoreLog().info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);

        ElasticSearchSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, dataMetricEvent,
                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    String json = JsonObject.create().append("name", metric.getName()).append("time", metric.getTimestamp()).toString();
                    System.out.println(json);
                    try {
                        requestIndexer.add(Requests.indexRequest()
                                .index("aaron_test" + "_" + metric.getName())
                                .type("aaron_test")
                                .source(json.getBytes("UTF-8"), XContentType.JSON));
                    } catch (UnsupportedEncodingException e) {
                        LogUtil.getCoreLog().error(e.getMessage());
                    }
                },
                parameterTool);
        env.execute("flink learning connectors es7");
    }
}