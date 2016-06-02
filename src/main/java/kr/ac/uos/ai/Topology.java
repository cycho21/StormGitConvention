package kr.ac.uos.ai;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author Chan Yeon, Cho
 * @version 0.0.1 - SnapShot
 *          on 2016-05-18 enemy
 * @link http://ai.uos.ac.kr:9000/lovebube/UIMA_Management_Client
 */

public class Topology {

    public Topology() {
    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("HelloSpout", new HelloSpout(), 10);
        builder.setBolt("HelloBolt", new HelloBolt(), 3).shuffleGrouping("HelloSpout");
        builder.setBolt("HelloBolt", new HelloBolt(), 2).shuffleGrouping("HelloSpout");
        Config conf = new Config();
        // Submit topology to cluster
        try {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } catch (AlreadyAliveException ae) {
            System.out.println(ae);
        } catch (InvalidTopologyException ie) {
            System.out.println(ie);
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
}
