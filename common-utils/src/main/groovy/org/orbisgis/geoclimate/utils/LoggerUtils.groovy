package org.orbisgis.geoclimate.utils

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory

class LoggerUtils {



    /**
     * Utility class to change log level for all loggers
     *
     */
    static void setLoggerLevel(String loggerLevel) {
        if (loggerLevel) {
            Level level
            if (loggerLevel.equalsIgnoreCase("INFO")) {
                level = Level.INFO
            } else if (loggerLevel.equalsIgnoreCase("DEBUG")) {
                level = Level.DEBUG
            } else if (loggerLevel.equalsIgnoreCase("TRACE")) {
                level = Level.TRACE
            } else if (loggerLevel.equalsIgnoreCase("OFF")) {
                level = Level.OFF
            } else {
                throw new RuntimeException("Invalid log level. Allowed values are : INFO, DEBUG, TRACE, OFF")
            }
            var logFac =  LoggerFactory.getILoggerFactory()
            if(logFac instanceof  LoggerContext){
                var context = (LoggerContext) LoggerFactory.getILoggerFactory()
                context.getLoggerList().each { it -> it.setLevel(level) }
            }
        }
    }


    /**
     * Create a logback logger
     * @param aClass
     * @return
     */
    static Logger createLogger(Class aClass){
        LoggerContext context = new LoggerContext()
        return context.getLogger(aClass)
    }
}
