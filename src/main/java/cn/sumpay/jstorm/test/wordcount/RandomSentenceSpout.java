package cn.sumpay.jstorm.test.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * @author heyc
 * @date 2018/8/14 15:07
 */
public class RandomSentenceSpout extends BaseRichSpout {

    protected SpoutOutputCollector spoutOutputCollector;

    protected Random random;

    protected String[] sentences;

    protected void initSentence() {
        this.random = new Random();
        this.sentences = new String[]{
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and senven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"
        };
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.initSentence();
    }

    @Override
    public void nextTuple() {
        String sentence = sentences[random.nextInt(sentences.length)];
        spoutOutputCollector.emit(new Values(sentence));
        Utils.sleep(500);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
