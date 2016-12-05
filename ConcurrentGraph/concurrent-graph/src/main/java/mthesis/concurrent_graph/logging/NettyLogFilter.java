package mthesis.concurrent_graph.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class NettyLogFilter extends Filter<ILoggingEvent> {

  @Override
  public FilterReply decide(ILoggingEvent event) {    
    if (event.getLoggerName().startsWith("io.netty") && event.getLevel().toInteger() <= Level.INFO_INT) {
      return FilterReply.DENY;
    } else {
      return FilterReply.NEUTRAL;
    }
  }
}