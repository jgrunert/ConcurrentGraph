package mthesis.concurrent_graph.logging;

import java.util.Arrays;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class ErrOutFilter extends Filter<ILoggingEvent> {

	@Override
	public FilterReply decide(ILoggingEvent event) {
		if (!isStarted())
		{
			return FilterReply.NEUTRAL;
		}

		final LoggingEvent loggingEvent = (LoggingEvent) event;

		final List<Level> eventsToKeep = Arrays.asList(Level.WARN, Level.ERROR);
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