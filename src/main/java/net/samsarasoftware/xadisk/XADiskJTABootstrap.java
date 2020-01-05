package net.samsarasoftware.xadisk;

/*-
 * #%L
 * samsarasoftware-xadisk
 * %%
 * Copyright (C) 2014 - 2017 Pere Joseph Rodriguez
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.log4j.Logger;
import org.xadisk.bridge.proxies.interfaces.XADiskBasicIOOperations;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.bridge.proxies.interfaces.XASession;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;

public class XADiskJTABootstrap {

	protected static Logger  log=Logger.getLogger(XADiskJTABootstrap.class);
	public  Map<String, String> options;
	
	public void shutdown(String instanceId) {
		
		XAFileSystem nativeXAF = XAFileSystemProxy.getNativeXAFileSystemReference(instanceId);

        log.info("Shutting down XADisk instance: "+instanceId);
        try {
			nativeXAF.shutdown();
		} catch (IOException e) {
			log.error(e.getMessage(),e);
		}
        log.info("Shutdown completed of XADisk instance: "+instanceId);
	}

	public void start(Map<String,String> options) {
		String instanceId=options.get("instanceId");
		String xaDirectory=options.get("xaDirectory");
		String deadLockDetectorInterval=options.get("deadLockDetectorInterval");
		String lockTimeOut=options.get("lockTimeOut");
		String transactionLogFileMaxSize=options.get("transactionLogFileMaxSizeDirectory");
		String transactionTimeout=options.get("transactionTimeout");
		String rollbackRecoveredTransactions=options.get("rollbackRecoveredTransactions");
		String workManagerMaxPoolSize=options.get("workManagerMaxPoolSize");
		String workManagerKeepAliveTime=options.get("workManagerKeepAliveTime");
		String workManagerCorePoolSize=options.get("workManagerCorePoolSize");
		String synchronizeDirectoryChanges=options.get("synchronizeDirectoryChanges");
		String serverPort=options.get("serverPort");
		String serverAddress=options.get("serverAddress");
		String nonDirectBufferPoolSize=options.get("nonDirectBufferPoolSize");
		String maxNonPooledBufferSize=options.get("maxNonPooledBufferSize");
		String nonDirectBufferIdleTime=options.get("nonDirectBufferIdleTime");
		String enableClusterMode=options.get("enableClusterMode");
		String maximumConcurrentEventDeliveries=options.get("maximumConcurrentEventDeliveries");
		String directBufferPoolSize=options.get("directBufferPoolSize");
		String enableRemoteInvocations=options.get("enableRemoteInvocations");
		String directBufferIdleTime=options.get("directBufferIdleTime");
		String cumulativeBufferSizeForDiskWrite=options.get("cumulativeBufferSizeForDiskWrite");
		String clusterMasterPort=options.get("clusterMasterPort");
		String clusterMasterAddress=options.get("clusterMasterAddress");
		String bufferSize=options.get("bufferSize");
		String bufferPoolRelieverInterval=options.get("bufferPoolRelieverInterval");
		
		
		this.options=Collections.unmodifiableMap(options);
		
		if(instanceId==null){
			RuntimeException e=new RuntimeException("Cannot initialize a XADisk instance with null instanceId. Please configure the XAInstanceId servlet param.");
			log.error(e.getMessage(),e);
			throw e;
		}
		if(xaDirectory==null){
			RuntimeException e=new RuntimeException("Cannot initialize a XADisk instance with null directory path. Please configure the XADirectory servlet param.");
			log.error(e.getMessage(),e);
			throw e;
		}
		
		log.info("Starting XADisk instance: "+instanceId);

		String XADiskSystemDirectory = xaDirectory;
		StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XADiskSystemDirectory, instanceId);
		
		try{
			if(deadLockDetectorInterval==null)
				configuration.setDeadLockDetectorInterval(Integer.parseInt(deadLockDetectorInterval));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" DeadLockDetectorInterval :"+configuration.getDeadLockDetectorInterval());
		}
		
		try{
			if(lockTimeOut==null)
				configuration.setLockTimeOut(Integer.parseInt(lockTimeOut));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" LockTimeOut :"+configuration.getLockTimeOut());
		}
		
		
		try{
			if(transactionLogFileMaxSize==null)
				configuration.setTransactionLogFileMaxSize(Long.parseLong(transactionLogFileMaxSize));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" TransactionLogFileMaxSize :"+configuration.getTransactionLogFileMaxSize());
		}
		
		try{
			if(transactionTimeout==null)
				configuration.setTransactionTimeout(Integer.parseInt(transactionTimeout));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" TransactionTimeout :"+configuration.getTransactionTimeout());
		}
		
		try{
			if(bufferPoolRelieverInterval==null)
				configuration.setBufferPoolRelieverInterval(Integer.parseInt(bufferPoolRelieverInterval));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" bufferPoolRelieverInterval :"+configuration.getBufferPoolRelieverInterval());
		}		
		
		try{
			if(bufferSize==null)
				configuration.setBufferSize(Integer.parseInt(bufferSize));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" bufferSize :"+configuration.getBufferSize());
		}

		try{
			if(clusterMasterAddress==null)
				configuration.setClusterMasterAddress(clusterMasterAddress);
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" clusterMasterAddress :"+configuration.getClusterMasterAddress());
		}

		try{
			if(clusterMasterPort==null)
				configuration.setClusterMasterPort(Integer.parseInt(clusterMasterPort));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" clusterMasterPort :"+configuration.getClusterMasterPort());
		}

		try{
			if(cumulativeBufferSizeForDiskWrite==null)
				configuration.setCumulativeBufferSizeForDiskWrite(Integer.parseInt(cumulativeBufferSizeForDiskWrite));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" cumulativeBufferSizeForDiskWrite :"+configuration.getCumulativeBufferSizeForDiskWrite());
		}

		try{
			if(directBufferIdleTime==null)
				configuration.setDirectBufferIdleTime(Integer.parseInt(directBufferIdleTime));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" directBufferIdleTime :"+configuration.getDirectBufferIdleTime());
		}

		try{
			if(directBufferPoolSize==null)
				configuration.setDirectBufferPoolSize(Integer.parseInt(directBufferPoolSize));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" directBufferPoolSize :"+configuration.getDirectBufferPoolSize());
		}

		try{
			if(enableClusterMode==null)
				configuration.setEnableClusterMode(Boolean.parseBoolean(enableClusterMode));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" enableClusterMode :"+configuration.getEnableClusterMode());
		}
		
		try{
			if(enableRemoteInvocations==null)
				configuration.setEnableRemoteInvocations(Boolean.parseBoolean(enableRemoteInvocations));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" enableRemoteInvocations :"+configuration.getEnableRemoteInvocations());
		}
		
		try{
			if(lockTimeOut==null)
				configuration.setLockTimeOut(Integer.parseInt(lockTimeOut));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" TransactionTimeout :"+configuration.getLockTimeOut());
		}

		try{
			if(maximumConcurrentEventDeliveries==null)
				configuration.setMaximumConcurrentEventDeliveries(Integer.parseInt(maximumConcurrentEventDeliveries));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" maximumConcurrentEventDeliveries :"+configuration.getMaximumConcurrentEventDeliveries());
		}

		try{
			if(maxNonPooledBufferSize==null)
				configuration.setMaxNonPooledBufferSize(Long.parseLong(maxNonPooledBufferSize));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" maxNonPooledBufferSize :"+configuration.getMaxNonPooledBufferSize());
		}

		try{
			if(nonDirectBufferIdleTime==null)
				configuration.setNonDirectBufferIdleTime(Integer.parseInt(nonDirectBufferIdleTime));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" nonDirectBufferPoolSize :"+configuration.getNonDirectBufferIdleTime());
		}

		try{
			if(nonDirectBufferPoolSize==null)
				configuration.setNonDirectBufferPoolSize(Integer.parseInt(nonDirectBufferPoolSize));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" nonDirectBufferPoolSize :"+configuration.getNonDirectBufferPoolSize());
		}
		
		try{
			if(serverAddress==null)
				configuration.setServerAddress(serverAddress);
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" serverAddress :"+configuration.getServerAddress());
		}

		try{
			if(serverPort==null)
				configuration.setServerPort(Integer.parseInt(serverPort));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" serverPort :"+configuration.getServerPort());
		}
		
		try{
			if(synchronizeDirectoryChanges==null)
				configuration.setSynchronizeDirectoryChanges(Boolean.parseBoolean(synchronizeDirectoryChanges));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" synchronizeDirectoryChanges :"+configuration.getSynchronizeDirectoryChanges());
		}

		try{
			if(workManagerCorePoolSize==null)
				configuration.setWorkManagerCorePoolSize(Integer.parseInt(workManagerCorePoolSize));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" workManagerCorePoolSize :"+configuration.getWorkManagerCorePoolSize());
		}
		
		try{
			if(workManagerKeepAliveTime==null)
				configuration.setWorkManagerKeepAliveTime(Long.parseLong(workManagerKeepAliveTime));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" workManagerKeepAliveTime :"+configuration.getWorkManagerKeepAliveTime());
		}

		try{
			if(workManagerMaxPoolSize==null)
				configuration.setWorkManagerMaxPoolSize(Integer.parseInt(workManagerMaxPoolSize));
		}catch(Throwable t){
			log.error(t.getMessage(),t);
		}finally{
			 log.debug("XADisk instance "+instanceId+" workManagerMaxPoolSize :"+configuration.getWorkManagerMaxPoolSize());
		}


		XAFileSystem xafs = XAFileSystemProxy.bootNativeXAFileSystem(configuration);
		try {
			xafs.waitForBootup(10000L);
			 log.info("Started XADisk instance: "+instanceId);

		} catch (InterruptedException e) {
			log.error(e.getMessage(),e);
		}
		
		try{
			if(Boolean.parseBoolean(rollbackRecoveredTransactions)){
				rollbackRecoveredTransactions(instanceId,xafs);
			}
		}catch(Throwable t){
			rollbackRecoveredTransactions(instanceId,xafs);
		}finally{
			 
		}
		
	}
	
	protected void rollbackRecoveredTransactions(String instanceId,XAFileSystem xafs) {
		XAResource res=xafs.getXAResourceForRecovery();
		Xid[] xids;
		try {
			xids = res.recover(XAResource.TMSTARTRSCAN);
		
			for (Xid xid : xids) {
				log.debug("XADisk instance "+instanceId+" preparing rollback form recovered transactions :"+xid.toString());
				try {
					res.rollback(xid);
				} catch (XAException e) {
					log.error(e.getMessage(),e);
				}
			}
		} catch (XAException e1) {
			log.error(e1.getMessage(),e1);
		}

		
	}

	public static XADiskBasicIOOperations getInstance(String instanceId, boolean publishFileStateChangeEventsOnCommit) throws  RollbackException, NamingException, SystemException{
		try{
			XAFileSystem xafs = XAFileSystemProxy.getNativeXAFileSystemReference(instanceId); //(a Native or Remote reference).
			XADiskBasicIOOperations xaSession = xafs.createSessionForXATransaction();
			InitialContext ic = new InitialContext();
			TransactionManager tm = (TransactionManager) ic.lookup("java:/TransactionManager");
			Transaction tx1=tm.getTransaction();
			XAResource xarXADisk = ((XASession) xaSession).getXAResource();
			tx1.enlistResource(xarXADisk);
			xaSession.setPublishFileStateChangeEventsOnCommit(publishFileStateChangeEventsOnCommit);
			
			return xaSession;
			
		} catch (NamingException e) {
			log.error(e.getMessage(),e);
			throw e;
		} catch (SystemException e) {
			log.error(e.getMessage(),e);
			throw e;
		} catch (IllegalStateException e) {
			log.error(e.getMessage(),e);
			throw e;
		} catch (RollbackException e) {
			log.error(e.getMessage(),e);
			throw e;
		}
	}

}
