package com.ctrip.platform.dal.dao.datasource.cluster;

import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.exceptions.HostNotExpectedException;

import java.util.Set;

public class ReadCurrentZoneSlavesOnlyStrategy implements ReadStrategy {
    @Override
    public void init(Set<HostSpec> hostSpecs) {

    }

    @Override
    public HostSpec pickRead(DalHints dalHints) throws HostNotExpectedException {
        return null;
    }

    @Override
    public void onChange(Set<HostSpec> hostSpecs) {

    }

    @Override
    public void dispose() {

    }
}
