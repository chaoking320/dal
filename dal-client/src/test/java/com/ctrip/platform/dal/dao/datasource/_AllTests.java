package com.ctrip.platform.dal.dao.datasource;

import com.ctrip.platform.dal.dao.datasource.cluster.MultiHostClusterPropertiesAdapterTest;
import com.ctrip.platform.dal.dao.datasource.cluster.validator.MajorityHostValidatorTest;
import com.ctrip.platform.dal.dao.datasource.cluster.strategy.*;
import com.ctrip.platform.dal.dao.datasource.cluster.validator.SimpleHostValidatorTest;
import com.ctrip.platform.dal.dao.datasource.jdbc.DalConnectionTest;
import com.ctrip.platform.dal.dao.datasource.log.ClusterDbSqlContextTest;
import com.ctrip.platform.dal.dao.datasource.log.KeyedDbSqlContextTest;
import com.ctrip.platform.dal.dao.datasource.monitor.DefaultDataSourceMonitorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
		DatabasePoolConfigParserTest.class,
		RefreshableDataSourceTest.class,
		ForceSwitchableDataSourceTest.class,
		LocalizedDataSourceTest.class,
        DalConnectionTest.class,
		ClusterDbSqlContextTest.class,
		KeyedDbSqlContextTest.class,
		DefaultDataSourceMonitorTest.class,
		ApiDataSourceIdentityTest.class,
		OrderedAccessStrategyTest.class,
		LocalizedAccessStrategyTest.class,
		CompositeRoundRobinAccessStrategyTest.class,
		RoundRobinAccessStrategyTest.class,
		ShardMetaDividerTest.class,
		CompositeStrategyTransformerTest.class,
		SimpleHostValidatorTest.class,
		MajorityHostValidatorTest.class,
		MultiHostClusterPropertiesAdapterTest.class
})
public class _AllTests {

}
