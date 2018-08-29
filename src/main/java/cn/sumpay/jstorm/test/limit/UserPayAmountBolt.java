package cn.sumpay.jstorm.test.limit;

import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import cn.sumpay.jstorm.risk.base.bolt.AbstractTimeItemBolt;
import cn.sumpay.risk.base.method.SumNumber;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/29 11:11
 */
public class UserPayAmountBolt extends AbstractTimeItemBolt {

    private static Logger logger = LoggerFactory.getLogger(UserPayAmountBolt.class);

    @Override
    public void execute(final Tuple input) {
        try {
            Map<String, ?> event = (Map)input.getValue(0);
            logger.info("UserPayAmountBolt: {}", JSONObject.toJSONString(event));
            String bizCode = (String)event.get("bizCode");
            String payUserId = (String)event.get("payUserId");
            Integer transAmount = ((Number)event.get("transAmount")).intValue();
            if (!"T004".equals(bizCode)) {
                return;
            }
            merge(payUserId, new SumNumber(transAmount));
        } catch (Exception e) {
            throw new FailedException("UserPayAmountBolt 异常", e);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {

    }

    @Override
    protected Object expirePattern() {
        return "24ph";
    }

    @Override
    protected String itemKey() {
        return "同一用户X小时快捷支付金额";
    }

    @Override
    protected String primaryTag() {
        return "用户号";
    }
}
