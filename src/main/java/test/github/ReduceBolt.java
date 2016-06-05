package test.github;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 * @author Chan Yeon, Cho
 * @version 0.0.1 - Snapshot
 *          on 2016-06-05
 * @link http://github.com/lovebube
 */
public class ReduceBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private FileWriter fileWriter;
    private BufferedWriter bufferedWriter;
    private String date;
    private File file;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        date = dateFormat.format(cal.getTime());

        file = new File("/root/logs/testLog.log." + date);
        File directory = new File("/root/logs/");

        if (!directory.exists()) {
            directory.mkdir();
        }

        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String language = tuple.getString(0).toString();

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            if (fileWriter != null) {
            } else {
                fileWriter = new FileWriter(file.getAbsoluteFile(), true);
            }

            if (bufferedWriter != null) {
                bufferedWriter.write(language + "\n");
                bufferedWriter.flush();
            } else {
                bufferedWriter = new BufferedWriter(fileWriter);
                bufferedWriter.write(language + "\n");
                bufferedWriter.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("languageReduced"));
    }

}
