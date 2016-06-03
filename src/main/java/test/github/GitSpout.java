package test.github;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPInputStream;


/**
 * @author Chan Yeon, Cho
 * @version 0.0.1 - SnapShot
 *          on 2016-06-02 enemy
 */

public class GitSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private String currentLine;
    private String date;
    private BufferedReader bufferedReader;


    public GitSpout() {
        jsontGet();
        try {
            bufferedReader = new BufferedReader(new FileReader(date + "-15.json"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("originJSON"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        parse();
    }

    public void jsontGet() {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        date = dateFormat.format(cal.getTime());

        System.out.println(date);

        String urlString = "http://data.githubarchive.org/" + date + "-15.json.gz";

        try {
            File file = new File(date+ "-15.json.gz");
            URL url = new URL(urlString);
            FileUtils.copyURLToFile(url, file);
            gzUnpack();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void gzUnpack() {

        byte[] buffer = new byte[1024];
        try {
            GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(date + "-15.json.gz"));
            FileOutputStream out = new FileOutputStream(date + "-15.json");

            int len;

            while((len = gzis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }

            gzis.close();
            out.close();

            System.out.println("unpacking done");

            System.out.println("analysis start");

            System.out.println(System.getenv("user.dir"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parse() {
        try {
            currentLine = bufferedReader.readLine();
            this._collector.emit(new Values(currentLine));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
