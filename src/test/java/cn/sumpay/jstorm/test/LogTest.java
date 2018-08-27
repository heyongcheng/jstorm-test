package cn.sumpay.jstorm.test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author heyc
 * @date 2018/8/22 17:59
 */
public class LogTest {

    private static final Pattern pattern = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2}\\s+)?(\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s+\\[(\\w*)]\\s+\\[(.*)]\\s+([A-Z]{4,5})\\s+(.*)\\s+-\\s+.*\\[EXTRACT-(.{4})]\\s+(.*)\\s+-(.*)$");

    public static void main(String[] args) {
        String message = "14:07:00.369 [jstorm] [main] INFO  cn.sumpay.len.jstorm.KafkaLogsTest - [EXTRACT-T004] 支付信息明细 -kafka log test 0";
        Matcher matcher = pattern.matcher(message);
        if (matcher.matches()) {
            int count = matcher.groupCount();
            for (int i=0; i<=count; i++) {
                String group = matcher.group(i);
                System.out.println(group);
            }
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
            System.out.println(event);
        }
    }

}
