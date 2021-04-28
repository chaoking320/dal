package com.ctrip.platform.dal.dao.configure;

import com.ctrip.platform.dal.dao.datasource.DataSourceIdentity;
import com.ctrip.framework.dal.cluster.client.base.HostSpec;

/**
 * @author c7ch23en
 */
public interface DalExtendedPoolConfiguration {

    int getSessionWaitTimeout();

    DataSourceIdentity getDataSourceId();

    HostSpec getHost();

}
