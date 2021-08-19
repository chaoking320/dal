package com.ctrip.platform.dal.dao.datasource.cluster.validator;

import com.ctrip.framework.dal.cluster.client.base.HostSpec;
import com.ctrip.platform.dal.dao.base.MockDefaultHostConnection;
import com.ctrip.platform.dal.dao.datasource.cluster.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockMajorityHostValidator extends MajorityHostValidator {

    public volatile HashMap<HostSpec, MysqlStatus> mysqlServer = new HashMap<>();
    public volatile HashMap<HostSpec, Connection> connectionMap = new HashMap<>();

    {
        HostSpec hostSpec1 = HostSpec.of("local", 3306);
        HostSpec hostSpec2 = HostSpec.of("local", 3307);
        HostSpec hostSpec3 = HostSpec.of("local", 3308);
        mysqlServer.put(hostSpec1, MysqlStatus.ok);
        mysqlServer.put(hostSpec2, MysqlStatus.ok);
        mysqlServer.put(hostSpec3, MysqlStatus.ok);
    }

    enum MysqlStatus{
        ok, unknown, fail
    }

    public MockMajorityHostValidator(ConnectionFactory factory, Set<HostSpec> configuredHosts, List<HostSpec> orderHosts, long failOverTime, long blackListTimeOut, long fixedValidatePeriod) {
        super(factory, configuredHosts, orderHosts, failOverTime, blackListTimeOut, fixedValidatePeriod);
    }

    @Override
    protected ValidateResult validate(Connection connection, int clusterHostCount) throws SQLException {
        MockDefaultHostConnection mockDefaultHostConnection = (MockDefaultHostConnection) connection;
        HostSpec host = mockDefaultHostConnection.getHost();

        if (MysqlStatus.unknown.equals(mysqlServer.get(host)))
            throw new SQLException("");

        int onlineCount = 0;
        for (Map.Entry<HostSpec, MysqlStatus> entry : mysqlServer.entrySet()) {
            if (MysqlStatus.ok.equals(entry.getValue())) {
                onlineCount++;
            }
        }
        if (MysqlStatus.ok.equals(mysqlServer.get(host)) && 2 * onlineCount > mysqlServer.size()) {
            return new ValidateResult(true, "");
        } else {
            return new ValidateResult(false, "");
        }
    }

    @Override
    protected boolean doubleCheckOnlineStatus(String currentMemberId, HostSpec currentHostSpec) {
        return super.doubleCheckOnlineStatus(currentMemberId, currentHostSpec);
    }

    @Override
    protected Connection getConnection(HostSpec host) throws SQLException {
        return connectionMap.get(host);
    }
}
