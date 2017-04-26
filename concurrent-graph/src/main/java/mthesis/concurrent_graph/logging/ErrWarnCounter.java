package mthesis.concurrent_graph.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class ErrWarnCounter extends Filter<ILoggingEvent> {

	public static boolean Enabled = true;
	public static int Warnings = 0;
	public static int Errors = 0;

	@Override
	public FilterReply decide(ILoggingEvent event) {
		if(Enabled) {
			if(event.getLevel() == Level.WARN)
				Warnings++;
			else if(event.getLevel() == Level.ERROR)
				Errors++;
		}
		return FilterReply.NEUTRAL;
	}
}