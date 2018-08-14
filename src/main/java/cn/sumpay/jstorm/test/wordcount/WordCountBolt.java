package cn.sumpay.jstorm.test.wordcount;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author heyc
 * @date 2018/8/14 15:41
 */
@Slf4j
public class WordCountBolt extends BaseBasicBolt {

    protected Map<String, Long> counts = new ConcurrentHashMap<String, Long>();

    protected long delay = 2000;

    protected long lastTimestamp = System.currentTimeMillis();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0);
        Long count = tuple.getLong(1);
        counts.put(word, count);
        long current = System.currentTimeMillis();
        if (current - lastTimestamp > delay) {
            lastTimestamp = current;
            counts.entrySet().iterator();
            for (Map.Entry<String, Long> entry : counts.entrySet()) {
                log.info("word count result - {}:{}", entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
