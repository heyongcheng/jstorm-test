package cn.sumpay.jstorm.test.limit;

import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import cn.sumpay.jstorm.risk.base.bolt.AbstractCollectBolt;
import com.alibaba.fastjson.JSONObject;

import java.util.Date;
import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/29 20:10
 */
public class UserLastPayTimeBolt extends AbstractCollectBolt {

    @Override
    protected String itemKey() {
        return "用户最后一笔快捷支付时间";
    }

    @Override
    protected String primaryTag() {
        return "用户号";
    }

    @Override
    public void execute(final Tuple input) {
        try {
            Map<String, ?> event = (Map)input.getValue(0);
            logger.info("UserLastPayTimeBolt: {}", JSONObject.toJSONString(event));
            String bizCode = (String)event.get("bizCode");
            String payUserId = (String)event.get("payUserId");
            if (!"T004".equals(bizCode)) {
                return;
            }
            merge(payUserId, new Date());
        } catch (Exception e) {
            throw new FailedException("UserLastPayTimeBolt 异常", e);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {

    }
}
