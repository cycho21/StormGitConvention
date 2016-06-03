package test.github;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

/**
 * @author Chan Yeon, Cho
 * @version 0.0.1 - SnapShot
 *          on 2016-06-02 enemy
 */

public class GitAnalysis extends BaseRichBolt {
    private OutputCollector _collector;
    private JSONObject jsonObject;
    private JSONParser jsonParser;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        jsonParser = new JSONParser();
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        _collector.emit(tuple, new Values(tuple.getString(0) + " FUCK!!! "));
        _collector.ack(tuple);
    }
    // payload { pull_request { head { repo

    public void parse2data(String unparsedString) {
        Object obj = null;

        try {
            obj = jsonParser.parse(unparsedString);
            jsonObject = (JSONObject) obj;
            JSONObject payload = (JSONObject) jsonObject.get("payload");
            JSONObject pull_request = (JSONObject) payload.get("pull_request");

            if (pull_request != null) {
                JSONObject head = (JSONObject) pull_request.get("head");
                if (head != null) {
                    JSONObject repo = (JSONObject) head.get("repo");
                    if (repo != null) {
                        if(repo.get("language") != null) {
                        }
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("language"));
    }
}
