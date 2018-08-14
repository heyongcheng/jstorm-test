package cn.sumpay.jstorm.test.wordcount;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/14 15:25
 */
@Slf4j
public class FieldCountBolt extends BaseBasicBolt {

    protected Map<String, Long> counts = new HashMap<String, Long>();

    @Override
    public void prepare(final Map stormConf, final TopologyContext context) {
        super.prepare(stormConf, context);
        log.info("thread:{} object:{}", Thread.currentThread().getName(), this);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0);
        Long count = counts.get(word);
        if (count == null) {
            count = 0L;
        }
        counts.put(word, ++count);
        basicOutputCollector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
