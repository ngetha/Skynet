/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.saida.skynet.demo;

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
 *
 * @author Minas Tirith
 * 
 * Learning how to use Storm
 * This spout continously emits countries
 */
public class CountriesSpout extends BaseRichSpout{
    private SpoutOutputCollector  mCollector;
    private Random mRandom;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("country"));
    }

    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        mCollector = soc;
        mRandom = new Random();
    }

    @Override
    public void nextTuple() {
        //Sleep for a bit
        Utils.sleep(100);
        //The sentences that will be randomly emitted
        String[] sentences = new String[]{ "Kenya", "Uganda", "Tanzania", 
            "Morroco", "Tunisia", "Ethiopia", "Lithuania", "Rwanda" };
        //Randomly pick a sentence
        String sentence = sentences[mRandom.nextInt(sentences.length)];
        //Emit the sentence
        mCollector.emit(new Values(sentence));
    }
    
}
