package com.zdjizhi.storm.topology;

import com.zdjizhi.storm.bolt.CalculateBolt;
import com.zdjizhi.storm.bolt.GatheringBolt;
import com.zdjizhi.storm.bolt.ResultSendBolt;
import com.zdjizhi.storm.bolt.ParseKvBolt;
import com.zdjizhi.storm.spout.CustomizedKafkaSpout;
import com.zdjizhi.storm.utils.common.StreamAggregateConfig;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @ClassNameFlowAggregateTopo
 * @Author lixkvip@126.com
 * @Date2020/6/24 10:14
 * @Version V1.0
 **/
public class StreamAggregateTopology {

    private static final Log logger = LogFactory.get();
    private final String topologyName;
    private final Config topologyConfig;
    private TopologyBuilder builder;

    private StreamAggregateTopology() {
        this(StreamAggregateTopology.class.getSimpleName());
    }

    private StreamAggregateTopology(String topologyName) {
        this.topologyName = topologyName;
        topologyConfig = createTopologConfig();
    }

    /**
     * 测试配置
     * conf.setTopologyWorkerMaxHeapSize(6144);
     * conf.put(Config.WORKER_CHILDOPTS, "-Xmx4G -Xms2G");
     */
    private Config createTopologConfig() {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(60);
        conf.setMaxSpoutPending(StreamAggregateConfig.SPOUT_PARALLELISM);
        conf.setNumAckers(StreamAggregateConfig.TOPOLOGY_NUM_ACKS);
        return conf;
    }

    private void runLocally() throws InterruptedException {
        topologyConfig.setMaxTaskParallelism(1);
        StormRunner.runTopologyLocally(builder, topologyName, topologyConfig, 6000);
    }

    private void runRemotely() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        topologyConfig.setNumWorkers(StreamAggregateConfig.TOPOLOGY_WORKERS);
        //设置过高会导致很多问题，如心跳线程饿死、吞吐量大幅下跌
        topologyConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 8);
        StormRunner.runTopologyRemotely(builder, topologyName, topologyConfig);
    }

    private void buildTopology() {
        builder = new TopologyBuilder();
        builder.setSpout("CustomizedKafkaSpout", new CustomizedKafkaSpout(), StreamAggregateConfig.SPOUT_PARALLELISM);

        builder.setBolt("ParseKvBolt", new ParseKvBolt(), StreamAggregateConfig.PARSE_BOLT_PARALLELISM)
                .localOrShuffleGrouping("CustomizedKafkaSpout");

        builder.setBolt("CalculateBolt", new CalculateBolt(), StreamAggregateConfig.CALCULATE_BOLT_PARALLELISM)
                .localOrShuffleGrouping("ParseKvBolt");

        builder.setBolt("GatheringBolt", new GatheringBolt(), StreamAggregateConfig.GATHERING_BOLT_PARALLELISM)
                .fieldsGrouping("CalculateBolt", new Fields("dimensions"));

        builder.setBolt("ResultSendBolt", new ResultSendBolt(), StreamAggregateConfig.KAFKA_BOLT_PARALLELISM)
                .localOrShuffleGrouping("GatheringBolt");

    }

    public static void main(String[] args) throws Exception {
        StreamAggregateTopology streamAggregateTopology;
        boolean runLocally = true;
        int size = 2;
        if (args.length >= size && StreamAggregateConfig.MODEL.equalsIgnoreCase(args[1])) {
            runLocally = false;
            streamAggregateTopology = new StreamAggregateTopology(args[0]);
        } else {
            streamAggregateTopology = new StreamAggregateTopology();
        }

        streamAggregateTopology.buildTopology();

        if (runLocally) {
            logger.info("执行本地模式...");
            streamAggregateTopology.runLocally();
        } else {
            logger.info("执行远程部署模式...");
            streamAggregateTopology.runRemotely();
        }
    }
}
