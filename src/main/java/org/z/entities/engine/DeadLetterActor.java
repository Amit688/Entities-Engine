package org.z.entities.engine;

import akka.actor.AbstractActor; 
import akka.actor.DeadLetter;

public class DeadLetterActor extends AbstractActor {
	
	  @Override
	  public Receive createReceive() {
	    return receiveBuilder()
	      .match(DeadLetter.class, msg -> {
	        System.out.println("I am a dead letter "+msg.toString());
	        System.out.println("message "+msg.message().toString());
	        System.out.println("recipient "+msg.recipient().toString());
	        System.out.println("sender "+msg.sender().toString()); 
	        
	      })
	      .build();
	  }
	}