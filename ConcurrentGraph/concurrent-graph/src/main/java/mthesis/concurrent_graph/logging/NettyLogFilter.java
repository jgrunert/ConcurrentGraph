package mthesis.concurrent_graph.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import mthesis.concurrent_graph.Settings;

public class NettyLogFilter extends Filter<ILoggingEvent> {

  @Override
  public FilterReply decide(ILoggingEvent event) {    
    if (event.getLoggerName().startsWith("io.netty") && event.getLevel().toInteger() <= Settings.LOG_LEVEL_NETTY) {
      return FilterReply.DENY;
    } else {
      return FilterReply.NEUTRAL;
    }
  }
}