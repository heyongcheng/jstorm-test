package cn.sumpay.jstorm.test.limit;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.window.BaseWindowedBolt;
import com.alibaba.jstorm.window.Time;
import com.alibaba.jstorm.window.TimeWindow;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/21 9:54
 */
@Slf4j
public class CountWindowBolt extends BaseWindowedBolt<Tuple> {

    private OutputCollector collector;

    private long windowSize;

    private long windowSlide;

    @Override
    public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
        super.timeWindow(Time.milliseconds(windowSize), Time.milliseconds(windowSlide));
        this.collector = collector;
    }

    @Override
    public Object initWindowState(final TimeWindow window) {
        return new HashMap<String, Integer>();
    }

    @Override
    public void execute(final Tuple tuple, final Object state, final TimeWindow window) {
        Map<String, Integer> counts = (Map<String, Integer>) state;
        Map<String, ?> event = (Map)tuple.getValue(0);
        String bizCode = (String)event.get("bizCode");
        String payUserId = (String)event.get("payUserId");
        Integer transAmount = (Integer)event.get("transAmount");
        if (!"T004".equals(bizCode)) {
            return;
        }
        Integer count = counts.get(payUserId);
        if (count == null) {
            count = 0;
        }
        counts.put(payUserId, count + transAmount);
        log.info("execute counts. user: {}  transAmount: {}", payUserId, count + transAmount);
    }

    @Override
    public void purgeWindow(final Object state, final TimeWindow window) {
        Map<String, Integer> counts = (Map<String, Integer>) state;
        log.info("purging window: {}" , window);
        log.info("=============================");
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            log.info("userId: {} transAmount: {}",entry.getKey(), entry.getValue());
        }
        log.info("=============================");
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(final long windowSize) {
        this.windowSize = windowSize;
    }

    public long getWindowSlide() {
        return windowSlide;
    }

    public void setWindowSlide(final long windowSlide) {
        this.windowSlide = windowSlide;
    }
}
