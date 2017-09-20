package org.z.entities.engine.utils;

import joptsimple.internal.Strings;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Utils {
	
	/**
	 * The option are - 
	 * Trace < Debug < Info < Warn < Error < Fatal. 
	 * Trace is of the lowest priority and Fatal is having highest priority.  
	 * When we define logger level, anything having higher priority logs are also getting printed
	 * 
	 * @param debugLevel
	 */
	public static void setDebugLevel(Logger logger) {

		String debugLevel = System.getenv("DEBUG_LEVEL");		
		if( Strings.isNullOrEmpty(debugLevel)) {
			debugLevel = "ALL";
		}

		switch (debugLevel) {

		case "ALL":
			logger.setLevel(Level.ALL);
			break;
		case "DEBUG":
			logger.setLevel(Level.DEBUG);
			break;
		case "ERROR":
			logger.setLevel(Level.ERROR);
			break;
		case "WARNING":
			logger.setLevel(Level.WARN); 
			break;
		default:
			logger.setLevel(Level.ALL);
		} 
	}  
}
