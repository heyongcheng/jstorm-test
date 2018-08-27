package cn.sumpay.jstorm.test.limit;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/21 9:54
 */
@Slf4j
public class CountBolt extends BaseBasicBolt {

    private Map<String, Integer> countMap = new HashMap<String, Integer>();

    @Override
    public void execute(final Tuple input, final BasicOutputCollector collector) {
        Map<String, ?> event = (Map)input.getValue(0);
        String bizCode = (String)event.get("bizCode");
        String payUserId = (String)event.get("payUserId");
        Integer transAmount = ((Number)event.get("transAmount")).intValue();
        if (!"T004".equals(bizCode)) {
            return;
        }
        Integer count = countMap.get(payUserId);
        if (count == null) {
            count = 0;
        }
        countMap.put(payUserId, count + transAmount);
        log.info("execute counts. user: {}  transAmount: {}", payUserId, count + transAmount);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {

    }
}
