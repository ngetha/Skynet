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
import java.util.Random;

/**
 *
 * @author Minas Tirith
 * This bolt takes in countries and outputs their capital cities 
 * and their population
 */
public class CapitalBolt extends BaseBasicBolt{

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("country","capital", "population"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        //Get the sentence content from the tuple
        String country = tuple.getString(0);
        // cities and population
        HashMap<String,String> cities = new HashMap<>();
        cities.put("Kenya", "Nairobi");
        cities.put("Uganda", "Entebbe");
        cities.put("Tanzania", "Dar Es Salaam");
        cities.put("Morroco", "Casa Blanca");
        cities.put("Tunisia", "Tunis");
        cities.put("Ethiopia", "Ethiopia");
        cities.put("Lithuania", "Lithu");
        cities.put("Rwanda", "Kigali");        
        
        // get them
        String capital = cities.get(country);
        Integer cPop = new Random().nextInt(999999);
        Random r = new Random();
        if(r.nextGaussian() > 0.5){
           capital+="-II";
        }
        // emit
        boc.emit(new Values(country, capital, cPop));
    }
    
}
