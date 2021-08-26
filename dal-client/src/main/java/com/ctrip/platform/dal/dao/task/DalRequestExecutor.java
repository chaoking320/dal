package com.ctrip.platform.dal.dao.task;

import com.ctrip.platform.dal.cluster.util.StringUtils;
import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.client.DalLogger;
import com.ctrip.platform.dal.dao.client.LogContext;
import com.ctrip.platform.dal.dao.client.LogEntry;
import com.ctrip.platform.dal.dao.configure.DalConfigure;
import com.ctrip.platform.dal.dao.configure.DalThreadPoolExecutorConfigBuilder;
import com.ctrip.platform.dal.dao.configure.DatabaseSet;
import com.ctrip.platform.dal.dao.helper.DalElementFactory;
import com.ctrip.platform.dal.dao.log.DalLogTypes;
import com.ctrip.platform.dal.dao.log.ILogger;
import com.ctrip.platform.dal.exceptions.DalException;
import com.ctrip.platform.dal.exceptions.ErrorCode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Common request executor that support execute request that is of pojo or
 * sql in single, all or multiple shards
 * 
 * @author jhhe
 */
public class DalRequestExecutor {

	private static final ILogger LOGGER = DalElementFactory.DEFAULT.getILogger();

	private static AtomicReference<ExecutorService> serviceRef = new AtomicReference<>();

	public static final String MAX_POOL_SIZE = "maxPoolSize";
	public static final String KEEP_ALIVE_TIME = "keepAliveTime";
	public static final String WORK_QUEUE_CAPACITY = "workQueueCapacity";
	public static final String ALLOW_CORE_THREAD_TIMEOUT = "allowCoreThreadTimeOut";
	public static final String MAX_THREADS_PER_SHARD = "maxThreadsPerShard";
	
	// To be consist with default connection max active size
	public static final int DEFAULT_MAX_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
	public static final int DEFAULT_KEEP_ALIVE_TIME = 10;
	public static final int DEFAULT_WORK_QUEUE_CAPACITY = 0;
	public static final boolean DEFAULT_ALLOW_CORE_THREAD_TIMEOUT = true;
	public static final int DEFAULT_MAX_THREADS_PER_SHARD = 0;
	
	private DalLogger logger = DalClientFactory.getDalLogger();

	public static void init(String maxPoolSizeStr, String keepAliveTimeStr){
		if(serviceRef.get() != null)
			return;

		synchronized (DalRequestExecutor.class) {
			if(serviceRef.get() != null)
				return;

			int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
			if(maxPoolSizeStr != null)
				maxPoolSize = Integer.parseInt(maxPoolSizeStr);

			int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;
			if(keepAliveTimeStr != null)
				keepAliveTime = Integer.parseInt(keepAliveTimeStr);

			ThreadPoolExecutor executer = new ThreadPoolExecutor(maxPoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
				AtomicInteger atomic = new AtomicInteger();
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "DalRequestExecutor-Worker-" + this.atomic.getAndIncrement());
				}
			});
			executer.allowCoreThreadTimeOut(true);

			serviceRef.set(executer);
		}
	}
	        
	public static void init(DalConfigure configure) {
		if(serviceRef.get() != null)
			return;
		
		synchronized (DalRequestExecutor.class) {
			if(serviceRef.get() != null)
				return;
			
			int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
			String maxPoolSizeStr = configure.getFactory().getProperty(MAX_POOL_SIZE);
			if(!StringUtils.isEmpty(maxPoolSizeStr)) {
				maxPoolSize = Integer.parseInt(maxPoolSizeStr);
				LOGGER.logEvent(DalLogTypes.DAL_VALIDATION,
						"MaxPoolSize:" + maxPoolSize, "");
			}
			
			int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;
			String keepAliveTimeStr = configure.getFactory().getProperty(KEEP_ALIVE_TIME);
			if(!StringUtils.isEmpty(keepAliveTimeStr)) {
				keepAliveTime = Integer.parseInt(keepAliveTimeStr);
				LOGGER.logEvent(DalLogTypes.DAL_VALIDATION,
						"KeepAliveTime:" + keepAliveTime, "");
			}

			int workQueueCapacity = DEFAULT_WORK_QUEUE_CAPACITY;
			String workQueueCapacityStr = configure.getFactory().getProperty(WORK_QUEUE_CAPACITY);
			if(!StringUtils.isEmpty(workQueueCapacityStr)) {
				workQueueCapacity = Integer.parseInt(workQueueCapacityStr);
				LOGGER.logEvent(DalLogTypes.DAL_VALIDATION,
						"WorkQueueCapacity:" + workQueueCapacity, "");
			}

			boolean allowCoreThreadTimeOut = DEFAULT_ALLOW_CORE_THREAD_TIMEOUT;
			String allowCoreThreadTimeOutStr = configure.getFactory().getProperty(ALLOW_CORE_THREAD_TIMEOUT);
			if(!StringUtils.isEmpty(allowCoreThreadTimeOutStr)) {
				allowCoreThreadTimeOut = Boolean.parseBoolean(allowCoreThreadTimeOutStr);
				LOGGER.logEvent(DalLogTypes.DAL_VALIDATION,
						"AllowCoreThreadTimeOut:" + allowCoreThreadTimeOut, "");
			}

			int maxThreadsPerShard = DEFAULT_MAX_THREADS_PER_SHARD;
			String maxThreadsPerShardStr = configure.getFactory().getProperty(MAX_THREADS_PER_SHARD);
			if(!StringUtils.isEmpty(maxThreadsPerShardStr)) {
				maxThreadsPerShard = Integer.parseInt(maxThreadsPerShardStr);
				LOGGER.logEvent(DalLogTypes.DAL_VALIDATION,
						"MaxThreadsPerShard::Global:" + maxThreadsPerShard, "");
			}

			DalThreadPoolExecutorConfigBuilder builder = new DalThreadPoolExecutorConfigBuilder()
					.setCorePoolSize(maxPoolSize)
					.setMaxPoolSize(maxPoolSize)
					.setKeepAliveSeconds(keepAliveTime)
					.setMaxThreadsPerShard(maxThreadsPerShard);

			for (String dbSetName : configure.getDatabaseSetNames()) {
				DatabaseSet dbSet = configure.getDatabaseSet(dbSetName);
				Integer dbMaxThreadsPerShard = dbSet.getSettingAsInt(MAX_THREADS_PER_SHARD);
				if (dbMaxThreadsPerShard != null) {
					builder.setMaxThreadsPerShard(dbSetName, dbMaxThreadsPerShard);
					LOGGER.logEvent(DalLogTypes.DAL_VALIDATION,
							String.format("MaxThreadsPerShard::%s:%d", dbSetName, dbMaxThreadsPerShard), "");
				}
			}

            ThreadPoolExecutor executor = new DalThreadPoolExecutor(builder.build(),
					workQueueCapacity > 0 ? new LinkedBlockingQueue<>(workQueueCapacity) : new LinkedBlockingQueue<>(),
					new ThreadFactory() {
                final AtomicInteger atomic = new AtomicInteger();
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "DalRequestExecutor-Worker-" + this.atomic.getAndIncrement());
                }
            });
            executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
            
            serviceRef.set(executor);
		}
	}
	
	public static void shutdown() {
		if (serviceRef.get() == null)
			return;
		
		synchronized (DalRequestExecutor.class) {
			if (serviceRef.get() == null)
				return;
			
			serviceRef.get().shutdown();
			serviceRef.set(null);
		}
	}

	public <T> T execute(final DalHints hints, final DalRequest<T> request) throws SQLException {
		return execute(hints, request, null);
	}

	public <T> T execute(final DalHints hints, final DalRequest<T> request, final ShardExecutionCallback<T> callback) throws SQLException {
		return execute(hints, request, false, callback);
	}
	
	public <T> T execute(final DalHints hints, final DalRequest<T> request, final boolean nullable) throws SQLException {
		return execute(hints, request, nullable, null);
	}

	public <T> T execute(final DalHints hints, final DalRequest<T> request, final boolean nullable, final ShardExecutionCallback<T> callback) throws SQLException {
		if (hints.isAsyncExecution()) {
			Future<T> future = serviceRef.get().submit(new Callable<T>() {
				public T call() throws Exception {
					return internalExecute(hints, request, nullable, callback);
				}
			});

			if(hints.isAsyncExecution())
				hints.set(DalHintEnum.futureResult, future);
			return null;
		}

		return internalExecute(hints, request, nullable, callback);
	}

	private <T> T internalExecute(DalHints hints, DalRequest<T> request, boolean nullable, final ShardExecutionCallback<T> callback) throws SQLException {
		T result = null;
		Throwable error = null;
		
		LogContext logContext = logger.start(request);
		long startTime = System.currentTimeMillis();

		try {
			request.validateAndPrepare();
			
			if(request.isCrossShard())
				result = crossShardExecute(logContext, hints, request, callback);
			else
				result = nonCrossShardExecute(logContext, hints, request, callback);

			if(result == null && !nullable)
				throw new DalException(ErrorCode.AssertNull);

			request.endExecution();
		} catch (Throwable e) {
			error = e;
		}

		logContext.setDaoExecuteTime(System.currentTimeMillis() - startTime);
		logger.end(logContext, error);
		
		handleCallback(hints, result, error);
		if(error != null)
			throw DalException.wrap(error);
		
		return result;
	}

	private <T> T nonCrossShardExecute(LogContext logContext, DalHints hints, DalRequest<T> request, ShardExecutionCallback<T> callback) throws Exception {
        logContext.setSingleTask(true);
		String logicDbName = request.getLogicDbName();
		TaskCallable<T> task = request.createTask();
		String shard = task.getPreparedDbShard();
	    Callable<T> taskWrapper = new RequestTaskWrapper<T>(logicDbName, shard, task, logContext);
		T result = null;
		ShardExecutionResult<T> executionResult;
		Throwable error = null;
		try {
			result = taskWrapper.call();
			executionResult = new ShardExecutionResultImpl<>(null, null, result);
		} catch (Throwable t) {
			error = t;
			executionResult = new ShardExecutionResultImpl<>(null, null, task, error);
		}

		hints.handleError("There is an error during nonCrossShard execution: ", error, callback, executionResult);

		logContext.setStatementExecuteTime(task.getDalTaskContext().getStatementExecuteTime());
		logContext.setEntries(toList(task.getDalTaskContext().getLogEntry()));
		return result;
	}
	
	private <T> T crossShardExecute(LogContext logContext, DalHints hints, DalRequest<T> request, ShardExecutionCallback<T> callback) throws Exception {
        Map<String, TaskCallable<T>> tasks = request.createTasks();
        logContext.setShards(tasks.keySet());

        boolean isSequentialExecution = hints.is(DalHintEnum.sequentialExecution);
        logContext.setSeqencialExecution(isSequentialExecution);
        
        ResultMerger<T> merger = request.getMerger();
        String logicDbName = request.getLogicDbName();
        
	    logger.startCrossShardTasks(logContext, isSequentialExecution);
		
		T result = null;
		Throwable error = null;

		try {
            result = isSequentialExecution?
            		sequentialExecute(hints, logicDbName, tasks, merger, logContext, callback):
            		parallelExecute(hints, logicDbName, tasks, merger, logContext, callback);

        } catch (Throwable e) {
            error = e;
        }
		
		logger.endCrossShards(logContext, error);

		if(error != null)
            throw DalException.wrap(error);
		
		return result;

	}

	private <T> void handleCallback(final DalHints hints, T result, Throwable error) {
		DalResultCallback qc = (DalResultCallback)hints.get(DalHintEnum.resultCallback);
		if (qc == null)
			return;
		
		if(error == null)
			qc.onResult(result);
		else
			qc.onError(error);
	}

	private <T> T parallelExecute(DalHints hints, String logicDbName, Map<String, TaskCallable<T>> tasks, ResultMerger<T> merger,
								  LogContext logContext, ShardExecutionCallback<T> callback) throws SQLException {
		Map<String, Future<T>> resultFutures = new HashMap<>();

		List<LogEntry> logEntries = new ArrayList<>();
		long maxStatementExecuteTime = 0;
		for(final String shard: tasks.keySet())
			resultFutures.put(shard, serviceRef.get().submit(new RequestTaskWrapper<T>(logicDbName,
					shard, tasks.get(shard), logContext)));

		for(Map.Entry<String, Future<T>> entry: resultFutures.entrySet()) {
			String dbShard = entry.getKey();
			Throwable error = null;
			ShardExecutionResult<T> executionResult;
			try {
				T partial = entry.getValue().get();
				merger.addPartial(dbShard, partial);
				// TODO: tableShard may be inaccurate
				executionResult = new ShardExecutionResultImpl<>(dbShard, null, partial);
			} catch (Throwable e) {
				error = e;
				executionResult = new ShardExecutionResultImpl<>(dbShard, null, tasks.get(dbShard), e);
			}
			hints.handleError("There is error during parallel execution: ", error, callback, executionResult);
			TaskCallable task = tasks.get(entry.getKey());
			maxStatementExecuteTime = Math.max(maxStatementExecuteTime, task.getDalTaskContext().getStatementExecuteTime());
			addLogEntry(logEntries, task.getDalTaskContext().getLogEntry());
		}

		logContext.setStatementExecuteTime(maxStatementExecuteTime);
		logContext.setEntries(logEntries);
		return merger.merge();
	}

	private <T> T sequentialExecute(DalHints hints, String logicDbName, Map<String, TaskCallable<T>> tasks, ResultMerger<T> merger,
									LogContext logContext, ShardExecutionCallback<T> callback) throws SQLException {
		long totalStatementExecuteTime = 0;
		List<LogEntry> logEntries = new ArrayList<>();
		for(final String shard: tasks.keySet()) {
			Throwable error = null;
			ShardExecutionResult<T> executionResult;
			try {
				T partial = new RequestTaskWrapper<T>(logicDbName, shard, tasks.get(shard), logContext).call();
				merger.addPartial(shard, partial);
				// TODO: tableShard may be inaccurate
				executionResult = new ShardExecutionResultImpl<>(shard, null, partial);
			} catch (Throwable e) {
				error = e;
				executionResult = new ShardExecutionResultImpl<>(shard, null, tasks.get(shard), e);
			}
			hints.handleError("There is error during sequential execution: ", error, callback, executionResult);
			TaskCallable task = tasks.get(shard);
			totalStatementExecuteTime += task.getDalTaskContext().getStatementExecuteTime();
			addLogEntry(logEntries, task.getDalTaskContext().getLogEntry());
		}

		logContext.setStatementExecuteTime(totalStatementExecuteTime);
		logContext.setEntries(logEntries);
		return merger.merge();
	}

	private void addLogEntry(List<LogEntry> logEntries, LogEntry logEntry) {
		if (logEntry != null) {
			logEntries.add(logEntry);
		}
	}
	
	public static int getPoolSize() {
	    ThreadPoolExecutor executer = (ThreadPoolExecutor)serviceRef.get();
	    if (serviceRef.get() == null)
            return 0;
	    
	    return executer.getPoolSize();
	}

	private List<LogEntry> toList(LogEntry logEntry) {
	    List<LogEntry> logEntries = new ArrayList<>();
	    if (logEntry != null) {
	        logEntries.add(logEntry);
        }
        return logEntries;
    }

    protected static ExecutorService getExecutor() {
		return serviceRef.get();
	}

}