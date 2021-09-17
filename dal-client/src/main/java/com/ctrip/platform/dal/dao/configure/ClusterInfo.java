package com.ctrip.platform.dal.dao.configure;

import com.ctrip.framework.dal.cluster.client.Cluster;
import com.ctrip.framework.dal.cluster.client.database.DatabaseRole;
import com.ctrip.framework.dal.cluster.client.util.StringUtils;
import com.ctrip.platform.dal.dao.datasource.DataSourceIdentity;
import com.ctrip.platform.dal.dao.datasource.IClusterDataSourceIdentity;
import com.ctrip.platform.dal.dao.datasource.log.ClusterDbSqlContext;
import com.ctrip.platform.dal.dao.datasource.log.SqlContext;

public class ClusterInfo {

    protected final String ID_FORMAT = "%s-%d-%s";

    protected String clusterName;
    protected Integer shardIndex;
    protected DatabaseRole role;
    protected boolean dbSharding;
    protected Cluster cluster;

    public ClusterInfo() {}

    public ClusterInfo(String clusterName, Integer shardIndex, DatabaseRole role, boolean dbSharding) {
        this(clusterName, shardIndex, role, dbSharding, null);
    }

    public ClusterInfo(String clusterName, Integer shardIndex, DatabaseRole role, boolean dbSharding, Cluster cluster) {
        this.clusterName = clusterName;
        this.shardIndex = shardIndex;
        this.role = role;
        this.dbSharding = dbSharding;
        this.cluster = cluster;
    }

    public ClusterInfo cloneMaster() {
        return new ClusterInfo(clusterName, shardIndex, DatabaseRole.MASTER, dbSharding, cluster);
    }

    public GroupClusterInfo cloneSlaveWithIndex(Integer slaveIndex) {
        return new GroupClusterInfo(clusterName, shardIndex, DatabaseRole.SLAVE, dbSharding, cluster, slaveIndex);
    }

    public String getClusterName() {
        return clusterName;
    }

    public Integer getShardIndex() {
        return shardIndex;
    }

    public DatabaseRole getRole() {
        return role;
    }

    public boolean dbSharding() {
        return dbSharding;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public DataSourceIdentity toDataSourceIdentity() {
        return new SimpleClusterDataSourceIdentity(this, cluster);
    }

    @Override
    public String toString() {
        return String.format(ID_FORMAT, clusterName, shardIndex, role != null ? role.getValue() : null);
    }

    static class SimpleClusterDataSourceIdentity implements DataSourceIdentity, IClusterDataSourceIdentity {

        private ClusterInfo clusterInfo;
        private Cluster cluster;
        private String id;

        public SimpleClusterDataSourceIdentity(ClusterInfo clusterInfo, Cluster cluster) {
            this.clusterInfo = clusterInfo;
            this.cluster = cluster;
            this.id = clusterInfo.toString();
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public SqlContext createSqlContext() {
            ClusterDbSqlContext context = new ClusterDbSqlContext(cluster, getShardIndex(), getDatabaseRole());
            if (cluster != null && cluster.getLocalizationConfig() != null)
                context.populateDbZone(cluster.getLocalizationConfig().getZoneId());
            return context;
        }

        @Override
        public String getClusterName() {
            return clusterInfo.getClusterName();
        }

        @Override
        public Integer getShardIndex() {
            return clusterInfo.getShardIndex();
        }

        @Override
        public DatabaseRole getDatabaseRole() {
            return clusterInfo.getRole();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SimpleClusterDataSourceIdentity) {
                String id = getId();
                String objId = ((SimpleClusterDataSourceIdentity) obj).getId();
                return (id == null && objId == null) || (id != null && id.equalsIgnoreCase(objId));
            }
            return false;
        }

        @Override
        public int hashCode() {
            String id = getId();
            return id != null ? id.hashCode() : 0;
        }

    }

}
