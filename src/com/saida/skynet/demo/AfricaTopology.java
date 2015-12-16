/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.saida.skynet.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 *
 * @author Minas Tirith
 */
public class AfricaTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException{
        //Used to build the topology
        TopologyBuilder builder = new TopologyBuilder();
        // Countris spout
        builder.setSpout("cities.spout", new CountriesSpout(), 5);
        // the first bolt
        builder.setBolt("capitals.bolt", new CapitalBolt(), 8)
                .shuffleGrouping("cities.spout");
        // then the 2nd bolt
        builder.setBolt("citypop.bolt", new CitySummingBolt(), 12)
                .fieldsGrouping("capitals.bolt", new Fields("capital"));
        // then the 3rd bolt
        builder.setBolt("countrypop.bolt", new CountrySummingBolt(), 12)
                .fieldsGrouping("capitals.bolt", new Fields("country"));
        
        //new configuration
        Config conf = new Config();
        conf.setDebug(true);
        
        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {
          //parallelism hint to set the number of workers
          conf.setNumWorkers(3);
          //submit the topology
          StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        //Otherwise, we are running locally
        else {
          //Cap the maximum number of executors that can be spawned
          //for a component to 3
          conf.setMaxTaskParallelism(3);
          //LocalCluster is used to run locally
          LocalCluster cluster = new LocalCluster();
          //submit the topology
          cluster.submitTopology("word-count", conf, builder.createTopology());
          //sleep
          Thread.sleep(10000);
          //shut down the cluster
          cluster.shutdown();
        }
    }
}
