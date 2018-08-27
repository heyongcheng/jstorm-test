package cn.sumpay.jstorm.test.limit;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author heyc
 * @date 2018/8/21 9:51
 */
public class LogParseBolt extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(LogParseBolt.class);

    private static final Pattern pattern = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2}\\s+)?(\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s+\\[(\\w*)]\\s+\\[(.*)]\\s+([A-Z]{4,5})\\s+(.*)\\s+-\\s+.*\\[EXTRACT-(.{4})]\\s+(.*)\\s+-(.*)(\\r\\n)?$");

    @Override
    public void execute(final Tuple input, final BasicOutputCollector collector) {
        String message = null;
        try {
            byte[] binary = input.getBinary(0);
            message = new String(binary, "UTF-8");
        } catch (Exception e) {
            logger.error("解析消息异常", e);
        }
        logger.info("receive message: {}", message);
        Matcher matcher = pattern.matcher(message);
        if (matcher.matches()) {
            Map<String, Object> event = new HashMap<String, Object>();
            String date = matcher.group(1);
            String time = matcher.group(2);
            // 日志时间
            event.put("logTime", date == null ?  time : date + " " + time);
            // 系统标志
            event.put("application", matcher.group(3));
            // 线程名
            event.put("threadName", matcher.group(4));
            // 日志级别
            event.put("logLevel", matcher.group(5));
            // 类名
            event.put("className", matcher.group(6));
            // 业务类型
            event.put("bizCode", matcher.group(7));
            // 描述
            event.put("description", matcher.group(8));
            // 消息
            event.put("message", matcher.group(9));
            // 发送消息
            collector.emit(new Values(event));
        } else {
            logger.error("can not parse message: {}", message);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }
}
