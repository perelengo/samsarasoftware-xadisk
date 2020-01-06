package net.samsarasoftware.xadisk;

/*-
 * #%L
 * samsarasoftware-xadisk
 * %%
 * Copyright (C) 2014 - 2020 Pere Joseph Rodriguez
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
