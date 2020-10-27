package cn.ac.iie.storm.bolt.print;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @ClassNamePrintBolt
 * @Author lixkvip@126.com
 * @Date2020/6/28 15:34
 * @Version V1.0
 **/
public class PrintBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(PrintBolt.class);
    private static long a;
    private long b;
    public static long c;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        logger.error("==================================一批数据=========================");

        a= System.currentTimeMillis();
        b= System.currentTimeMillis();
        c= System.currentTimeMillis();


        logger.error(Thread.currentThread() + "private static long a======:" + a);
        logger.error(Thread.currentThread() + "private long b======:" + b);
        logger.error(Thread.currentThread() + "public static long c======:" + c);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
