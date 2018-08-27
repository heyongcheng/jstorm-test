package cn.sumpay.jstorm.test.limit;

import cn.sumpay.jstorm.test.utils.ResourceUtils;
import com.alibaba.jstorm.kafka.KafkaConfig;
import com.alibaba.jstorm.kafka.KafkaSpout;

/**
 * @author heyc
 * @date 2018/8/16 20:34
 */
public class RiskDataSpout extends KafkaSpout {

    public RiskDataSpout(String configPath) {
        super(new KafkaConfig(ResourceUtils.readYamlAsProperties(configPath)));
    }

}
