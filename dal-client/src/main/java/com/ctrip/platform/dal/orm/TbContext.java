package com.ctrip.platform.dal.orm;

import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.helper.DalDefaultJpaParser;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TbContext<T> implements ITbContext<T> {

    private Class<T> clazz;
    private DalParser<T> parser;
    private DalTableDao<T> dalTableDao;
    CachedDict cachedDict = new CachedDict();

    public TbContext(Class<T> clazz){
        this.clazz = clazz;
        try {
            this.parser = new DalDefaultJpaParser<T>(clazz);
            this.dalTableDao = new DalTableDao<T>(parser);
            cachedDict.fillDict(clazz);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int insert(T entity) {
        try {
            return dalTableDao.insert(buildDalHint(entity,entity.getClass()),new KeyHolder(),entity);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int delete(T entity) {
        try {
            return dalTableDao.delete(buildDalHint(entity, entity.getClass()),entity);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int update(T entity) {
        try {
            return dalTableDao.update(buildDalHint(entity, entity.getClass()),entity);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T firstOrDefault(Consumer<T> make) {
        try {
            T obj = clazz.newInstance();
            make.accept(obj);
            BuildResult buildResult = buildQuery(obj);
            return dalTableDao.queryFirst(buildResult.whereClause,buildResult.parameters,new DalHints().setFields(buildResult.fields));
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<T> where(Consumer<T> make, int cnt) {
        try {
            T obj = clazz.newInstance();
            make.accept(obj);
//            BuildResult buildResult = buildQuery(obj);
            return dalTableDao.queryBy(obj,new DalHints());
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private BuildResult buildQuery(T obj) {
        StatementParameters parameters = new StatementParameters();
        Map<String, ?> fields = parser.getFields(obj);
        Map<String, ?> queryCriteria = dalTableDao.filterNullFileds(fields);
        dalTableDao.addParameters(parameters, queryCriteria);
        String whereClause = dalTableDao.buildWhereClause(queryCriteria);

        return new BuildResult(whereClause, parameters, fields);
    }

    private DalHints buildDalHint(T entity ,Class<?> clazz){
        Field shareKeyField = cachedDict.shareKeyDict.get(clazz);
        DalHints hins = new DalHints();
        if (shareKeyField != null) {
            Annotation[] annotations = shareKeyField.getAnnotations();
            ShareKey shareKeyAnnotation = null;

            for (Annotation annotation : annotations) {
                if (annotation instanceof ShareKey) {
                    shareKeyAnnotation = (ShareKey) annotation;
                    break;
                }
            }

            if (shareKeyAnnotation != null) {
                int mod = shareKeyAnnotation.mod();
                Object key = null;
                try {
                    key = shareKeyField.get(entity);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                if (key instanceof Long) {
                    hins.inTableShard((int) ((Long) key % mod));
                } else if (key instanceof Integer) {
                    hins.inTableShard((Integer) key % mod);
                }
            }
        }

        return hins;
    }

    static class BuildResult{
        private String whereClause;
        private StatementParameters parameters;
        Map<String, ?> fields;

        public BuildResult(String whereClause,StatementParameters parameters,Map<String, ?> fields){
            this.whereClause = whereClause;
            this.parameters = parameters;
            this.fields = fields;
        }

        public String getWhereClause() {
            return whereClause;
        }

        public void setWhereClause(String whereClause) {
            this.whereClause = whereClause;
        }

        public StatementParameters getParameters() {
            return parameters;
        }

        public void setParameters(StatementParameters parameters) {
            this.parameters = parameters;
        }

        public Map<String, ?> getFields() {
            return fields;
        }

        public void setFields(Map<String, ?> fields) {
            this.fields = fields;
        }
    }
}
