package test.github;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * @author Chan Yeon, Cho
 * @version 0.0.1 - SnapShot
 *          on 2016-06-02 enemy
 */

public class GitTopology {

    private final String[] args;

    public GitTopology(String[] args) {
        this.args = args;
        init();
    }

    private void init() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("language", new GitSpout(), 1);
        builder.setBolt("parse", new GitBolt(), 4).shuffleGrouping("language");
        builder.setBolt("analysis", new GitAnalysis(), 4).shuffleGrouping("parse");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("langLocal", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("langLocal");
            cluster.shutdown();
        }
    }

    public static void main(String[] args) {
        new GitTopology(args);
    }
}
