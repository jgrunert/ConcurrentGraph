package mthesis.concurrent_graph.logging;

import java.util.Arrays;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class StdOutFilter extends Filter<ILoggingEvent> {

	@Override
	public FilterReply decide(ILoggingEvent event) {
		final LoggingEvent loggingEvent = (LoggingEvent) event;

		final List<Level> eventsToKeep = Arrays.asList(Level.TRACE, Level.DEBUG, Level.INFO);
		if (eventsToKeep.contains(loggingEvent.getLevel()))
		{
			return FilterReply.NEUTRAL;
		}
		else
		{
			return FilterReply.DENY;
		}
	}
}