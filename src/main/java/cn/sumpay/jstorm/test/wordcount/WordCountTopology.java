package cn.sumpay.jstorm.test.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import lombok.SneakyThrows;

/**
 * @author heyc
 * @date 2018/8/14 15:06
 */
public class WordCountTopology {

    @SneakyThrows
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 2);
        builder.setBolt("split", new SplitSentenceBolt(), 2).localOrShuffleGrouping("spout");
        builder.setBolt("field", new FieldCountBolt(), 2).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("count", new WordCountBolt(), 1).globalGrouping("field");

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length != 0) {
            config.setNumWorkers(2);
            StormSubmitter.submitTopology("wordCountTopology", config, builder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordCountTopology", config, builder.createTopology());
            Thread.sleep(100000);
            localCluster.shutdown();
        }
    }

}
