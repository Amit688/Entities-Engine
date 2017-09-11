package org.z.entities.engine;

import akka.actor.AbstractActor; 
import akka.actor.DeadLetter;

public class DeadLetterActor extends AbstractActor {
	
	  @Override
	  public Receive createReceive() {
	    return receiveBuilder()
	      .match(DeadLetter.class, msg -> {
	        System.out.println("I am dead letter "+msg);
	      })
	      .build();
	  }
	}