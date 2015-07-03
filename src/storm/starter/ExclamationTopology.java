package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a basic example of a Storm topology.
 * spout随机产生单词，交给bolt，加一个感叹号，再交给bolt，再加一个感叹号
 */
public class ExclamationTopology {

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 2);
    builder.setBolt("exclaim1", new ExclamationBolt(), 2).shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
 class TestWordSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestWordSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public TestWordSpout() {
        this(true);
    }

    public TestWordSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {
        Utils.sleep(100);
        final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
        final Random rand = new Random();
        final String word = words[rand.nextInt(words.length)];
        _collector.emit(new Values(word));
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }    
}
 /*一些输出：
 14022 [Thread-15-exclaim2] INFO  backtype.storm.daemon.executor - Processing received message source: exclaim1:3, stream: default, id: {}, [bertels!]
 14022 [Thread-15-exclaim2] INFO  backtype.storm.daemon.task - Emitting: exclaim2 default [bertels!!]
 14022 [Thread-19-word] INFO  backtype.storm.daemon.task - Emitting: word default [golda]
 14022 [Thread-9-exclaim1] INFO  backtype.storm.daemon.executor - Processing received message source: word:7, stream: default, id: {}, [golda]
 14022 [Thread-9-exclaim1] INFO  backtype.storm.daemon.task - Emitting: exclaim1 default [golda!]
 14022 [Thread-15-exclaim2] INFO  backtype.storm.daemon.executor - Processing received message source: exclaim1:2, stream: default, id: {}, [golda!]
 14022 [Thread-15-exclaim2] INFO  backtype.storm.daemon.task - Emitting: exclaim2 default [golda!!]
 */
 /*
  * 输出说明：
  * 对Thread-15进行观察。对收到的tuple的字符串加一个感叹号后再喷射出去。
  */