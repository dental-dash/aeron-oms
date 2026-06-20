package com.oms.common;

public class DomainService {
    protected EventStream events;

    public DomainService(EventStream events) {
        this.events = events;
    }
}
