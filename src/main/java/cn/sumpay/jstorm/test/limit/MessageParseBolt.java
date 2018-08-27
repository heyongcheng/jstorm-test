package cn.sumpay.jstorm.test.limit;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/22 21:20
 */
@Slf4j
public class MessageParseBolt extends BaseBasicBolt {

    @Override
    public void execute(final Tuple input, final BasicOutputCollector collector) {
        Map event = (Map) input.getValue(0);
        String message = (String)event.get("message");
        log.info("receive message: {}", message);
        if (message != null && !"".equals(message)) {
            try {
                Gson gson = new Gson();
                event.putAll(gson.fromJson(message, Map.class));
                collector.emit(new Values(event));
            } catch (Exception e) {
                log.error("解析 message 异常", e);
            }
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }
}
