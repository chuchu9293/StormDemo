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
 * spout随机产生单词，交给bolt，加上叹号后输出
 * 使用命名stream
 */
public class ExclamationTopology2 {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("mySpout", new TestWordSpout2(), 4);
		builder.setBolt("myBolt", new ExclamationBolt2(), 2).fieldsGrouping(
				"mySpout", "streamId1", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}

	
}
class TestWordSpout2 extends BaseRichSpout {
	private static final long serialVersionUID = 7012309812739928986L;
	public static Logger LOG = LoggerFactory.getLogger(TestWordSpout.class);
	boolean _isDistributed;
	SpoutOutputCollector _collector;

	public TestWordSpout2() {
		this(true);
	}

	public TestWordSpout2(boolean isDistributed) {
		_isDistributed = isDistributed;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		final String[] words = new String[] { "nathan", "mike", "jackson",
				"golda", "bertels" };
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		_collector.emit("streamId1", new Values(word));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("streamId1",new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}
}
class ExclamationBolt2 extends BaseRichBolt {
	private static final long serialVersionUID = -951776470328476166L;
	OutputCollector _collector;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		//_collector.emit(tuple, new Values(tuple.getString(0) + "!"));
		System.out.println(tuple.getString(0) + "!");
		//_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}

/*
 * 一些输出： 14022 [Thread-15-exclaim2] INFO backtype.storm.daemon.executor -
 * Processing received message source: exclaim1:3, stream: default, id: {},
 * [bertels!] 14022 [Thread-15-exclaim2] INFO backtype.storm.daemon.task -
 * Emitting: exclaim2 default [bertels!!] 14022 [Thread-19-word] INFO
 * backtype.storm.daemon.task - Emitting: word default [golda] 14022
 * [Thread-9-exclaim1] INFO backtype.storm.daemon.executor - Processing received
 * message source: word:7, stream: default, id: {}, [golda] 14022
 * [Thread-9-exclaim1] INFO backtype.storm.daemon.task - Emitting: exclaim1
 * default [golda!] 14022 [Thread-15-exclaim2] INFO
 * backtype.storm.daemon.executor - Processing received message source:
 * exclaim1:2, stream: default, id: {}, [golda!] 14022 [Thread-15-exclaim2] INFO
 * backtype.storm.daemon.task - Emitting: exclaim2 default [golda!!]
 */
/*
 * 输出说明： 对Thread-15进行观察。对收到的tuple的字符串加一个感叹号后再喷射出去。
 */