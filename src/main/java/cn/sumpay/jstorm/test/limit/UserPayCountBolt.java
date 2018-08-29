package cn.sumpay.jstorm.test.limit;

import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import cn.sumpay.jstorm.risk.base.bolt.AbstractTimeItemBolt;
import cn.sumpay.risk.base.method.CountNumber;
import com.alibaba.fastjson.JSONObject;

import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/29 20:07
 */
public class UserPayCountBolt extends AbstractTimeItemBolt {

    @Override
    protected Object expirePattern() {
        return "24ph";
    }

    @Override
    protected String itemKey() {
        return "同一用户X小时快捷支付次数";
    }

    @Override
    protected String primaryTag() {
        return "用户号";
    }

    @Override
    public void execute(final Tuple input) {
        try {
            Map<String, ?> event = (Map)input.getValue(0);
            logger.info("UserPayAmountBolt: {}", JSONObject.toJSONString(event));
            String bizCode = (String)event.get("bizCode");
            String payUserId = (String)event.get("payUserId");
            if (!"T004".equals(bizCode)) {
                return;
            }
            merge(payUserId, new CountNumber(1L));
        } catch (Exception e) {
            throw new FailedException("UserPayAmountBolt 异常", e);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {

    }
}
