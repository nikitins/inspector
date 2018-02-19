package com.inspector;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DateInspector implements Interceptor {
    public void initialize() {
    }

    public Event intercept(Event event) {
        String body = new String(event.getBody());
        String[] fields = body.split(",");
        String dateTime = fields[2];

        String date = dateTime.split(" ")[1];
        String[] splitDate = date.split("\\.");
        String m = splitDate[0]; 
        String d = splitDate[1]; 
        String y = splitDate[2]; 

	Map<String, String> headers = event.getHeaders();
        headers.put("month", m);
        headers.put("day", d);
        headers.put("year", y);
        event.setHeaders(headers);
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        List<Event> interceptedEvents =
                new ArrayList<Event>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    public void close() {

    }

    public static class Builder
            implements Interceptor.Builder {

        @Override
        public void configure(Context context) {
        }

        @Override
        public Interceptor build() {
            return new DateInspector();
        }
    }
}
