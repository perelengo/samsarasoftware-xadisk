package net.samsarasoftware.xadisk;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.ResourceException;

import org.apache.log4j.Logger;
import org.xadisk.bridge.proxies.interfaces.XADiskBasicIOOperations;
import org.xadisk.connector.outbound.XADiskConnectionFactory;

public class XADiskJCABootstrap {

	protected static Logger  log=Logger.getLogger(XADiskJCABootstrap.class);
	

	public static XADiskBasicIOOperations getInstance(String jndi, boolean publishFileStateChangeEventsOnCommit) throws   NamingException, ResourceException{
		try{
			XADiskConnectionFactory cfLocal = (XADiskConnectionFactory) new InitialContext().lookup(jndi);
			XADiskBasicIOOperations connectionLocal = cfLocal.getConnection();
			
			if(publishFileStateChangeEventsOnCommit){
				connectionLocal.setPublishFileStateChangeEventsOnCommit(publishFileStateChangeEventsOnCommit);
			}
			
			return connectionLocal;
			
		} catch (NamingException e) {
			log.error(e.getMessage(),e);
			throw e;
		} catch (ResourceException e) {
			log.error(e.getMessage(),e);
			throw e;
		}
	}

}
