package com.ctrip.framework.dal.cluster.client.cluster;

import com.ctrip.framework.dal.cluster.client.Cluster;
import com.ctrip.framework.dal.cluster.client.base.UnsupportedListenable;
import com.ctrip.framework.dal.cluster.client.config.ClusterConfigImpl;
import com.ctrip.framework.dal.cluster.client.config.DalConfigCustomizedOption;
import com.ctrip.framework.dal.cluster.client.config.LocalizationConfig;
import com.ctrip.framework.dal.cluster.client.database.Database;
import com.ctrip.framework.dal.cluster.client.database.DatabaseCategory;
import com.ctrip.framework.dal.cluster.client.exception.ClusterRuntimeException;
import com.ctrip.framework.dal.cluster.client.multihost.ClusterRouteStrategyConfig;
import com.ctrip.framework.dal.cluster.client.shard.DatabaseShard;
import com.ctrip.framework.dal.cluster.client.sharding.context.DbShardContext;
import com.ctrip.framework.dal.cluster.client.sharding.context.TableShardContext;
import com.ctrip.framework.dal.cluster.client.sharding.idgen.ClusterIdGeneratorConfig;

import java.sql.SQLException;
import java.util.*;

/**
 * @author c7ch23en
 */
public class DefaultCluster extends UnsupportedListenable<ClusterSwitchedEvent> implements Cluster {

    private ClusterConfigImpl clusterConfig;
    private Map<Integer, DatabaseShard> databaseShards = new HashMap<>();
    private ShardStrategyProxy shardStrategyProxy;
    private ClusterIdGeneratorConfig idGeneratorConfig;
    private ClusterRouteStrategyConfig routeStrategyConfig;
    private LocalizationConfig localizationConfig;
    private LocalizationConfig lastLocalizationConfig;
    private DalConfigCustomizedOption customizedOption;

    public DefaultCluster(ClusterConfigImpl clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    @Override
    public String getClusterName() {
        return clusterConfig.getClusterName();
    }

    @Override
    public ClusterType getClusterType() {
        return clusterConfig.getClusterType();
    }

    @Override
    public DatabaseCategory getDatabaseCategory() {
        return clusterConfig.getDatabaseCategory();
    }

    @Override
    public boolean dbShardingEnabled() {
        return databaseShards.size() > 1;
    }

    @Override
    public Integer getDbShard(String tableName, DbShardContext context) {
        return shardStrategyProxy.getDbShard(tableName, context);
    }

    @Override
    public Set<Integer> getAllDbShards() {
        return databaseShards.keySet();
    }

    @Override
    public boolean tableShardingEnabled(String tableName) {
        return shardStrategyProxy.tableShardingEnabled(tableName);
    }

    @Override
    public String getTableShard(String tableName, TableShardContext context) {
        return shardStrategyProxy.getTableShard(tableName, context);
    }

    @Override
    public Set<String> getAllTableShards(String tableName) {
        return shardStrategyProxy.getAllTableShards(tableName);
    }

    @Override
    public String getTableShardSeparator(String tableName) {
        return shardStrategyProxy.getTableShardSeparator(tableName);
    }

    @Override
    public List<Database> getDatabases() {
        List<Database> databases = new LinkedList<>();
        for (DatabaseShard databaseShard : databaseShards.values()) {
            databases.addAll(databaseShard.getMasters());
            databases.addAll(databaseShard.getSlaves());
        }
        return databases;
    }

    @Override
    public Database getMasterOnShard(int shardIndex) {
        List<Database> masters = databaseShards.get(shardIndex).getMasters();
        if (masters == null || masters.isEmpty())
            throw new ClusterRuntimeException("no master available");
//        if (masters.size() > 1)
//            throw new ClusterRuntimeException("multi masters available");
        return masters.iterator().next();
    }

    @Override
    public List<Database> getSlavesOnShard(int shardIndex) {
        return databaseShards.get(shardIndex).getSlaves();
    }

    @Override
    public ClusterIdGeneratorConfig getIdGeneratorConfig() {
        return idGeneratorConfig;
    }

    @Override
    public ClusterRouteStrategyConfig getRouteStrategyConfig() {
        return routeStrategyConfig;
    }

    @Override
    public LocalizationConfig getLocalizationConfig() {
        return localizationConfig;
    }

    @Override
    public LocalizationConfig getLastLocalizationConfig() {
        return this.lastLocalizationConfig;
    }

    @Override
    public DalConfigCustomizedOption getCustomizedOption() {
        return this.customizedOption;
    }

    public void setCustomizedClass(DalConfigCustomizedOption customizedOption) {
        this.customizedOption = customizedOption;
    }

    public void setLastLocalizationConfig(LocalizationConfig lastConfig) {
        this.lastLocalizationConfig = lastConfig;
    }

    public void addDatabaseShard(DatabaseShard databaseShard) {
        databaseShards.put(databaseShard.getShardIndex(), databaseShard);
    }

    public void setShardStrategy(ShardStrategyProxy shardStrategyProxy) {
        this.shardStrategyProxy = shardStrategyProxy;
    }

    public void setIdGeneratorConfig(ClusterIdGeneratorConfig idGeneratorConfig) {
        this.idGeneratorConfig = idGeneratorConfig;
    }

    public void setRouteStrategyConfig(ClusterRouteStrategyConfig routeStrategyConfig) {
        this.routeStrategyConfig = routeStrategyConfig;
    }

    public void setLocalizationConfig(LocalizationConfig localizationConfig) {
        this.localizationConfig = localizationConfig;
    }

    public void validate() {}

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw new ClusterRuntimeException(String.format("Unable to unwrap %s to %s", this.toString(), iface.toString()));
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

}
