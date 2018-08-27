package cn.sumpay.jstorm.test.limit;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/22 21:17
 */
public class ExpandFieldBolt extends BaseBasicBolt {

    @Override
    public void execute(final Tuple input, final BasicOutputCollector collector) {
        Map event = (Map)input.getValue(0);
        event.put("expand", "expand-value");
        collector.emit(new Values(event));
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }
}
