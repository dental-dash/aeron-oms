package com.oms.common;

public class EventHandlerService {
    protected CommandStream commands;

    public EventHandlerService(CommandStream commands) {
        this.commands = commands;
    }
}
