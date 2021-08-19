package com.ctrip.platform.dal.dao.datasource.cluster.strategy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2021/8/18
 */
public class ShardMetaDividerTest extends ShardMetaGenerator {

    private ZoneDividedStrategyContext strategyContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        strategyContext = new ZoneDividedStrategyContext(shardMeta, connectionFactory, caseInsensitiveProperties);
    }

    @Test
    public void divide() {
        LocalizedAccessStrategy localizedAccessStrategy = (LocalizedAccessStrategy) strategyContext.accept(strategyTransformer);
        Assert.assertEquals(3, localizedAccessStrategy.size());
    }

}