/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.saida.skynet.demo;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;

/**
 *
 * @author Minas Tirith
 */
public class CountrySummingBolt extends BaseBasicBolt{
    HashMap<String, Integer> sums = new HashMap<>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("country", "population"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
      //Get the word contents from the tuple
      String country = tuple.getString(0);
      Integer pop = tuple.getInteger(2);
      //Have we counted any already?
      Integer count = sums.get(country);
      if (count == null)
        count = 0;
      //Increment the count and store it
      count+=pop;
      sums.put(country, count);
      //Emit the word and the current count
      boc.emit(new Values(country, count));
    }
    
}
