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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.xadisk.bridge.proxies.interfaces.XADiskBasicIOOperations;

public class XADiskBootstrap {

	protected static Logger  log=Logger.getLogger(XADiskBootstrap.class);
	
	public static Map<String, Object> instances=Collections.synchronizedMap(new HashMap<String, Object>());
	
	public void start(Map<String, String> options) throws Exception{
		if(options.get("XAInstanceJNDI")!=null){
			if(instances.get(options.get("XAInstanceJNDI"))==null){
				XADiskJCABootstrap instance = new XADiskJCABootstrap();
				instances.put(options.get("XAInstanceJNDI"), instance);
			}
		}else{
			if(instances.get(options.get("instanceId"))==null){
				XADiskJTABootstrap instance = new XADiskJTABootstrap();
				instance.start(options);
				instances.put(options.get("instanceId"), instance);
			}
		}
	}
	
	public void shutdown(String instanceId ){
		Object instance = instances.get(instanceId);
		if(instance!=null && instance instanceof XADiskJTABootstrap){
			((XADiskJTABootstrap)instance).shutdown(instanceId);
			instances.remove(instanceId);
		}
		
	}
	
	public static XADiskBasicIOOperations getInstance(String instanceId, boolean publishFileStateChangeEventsOnCommit) throws  Exception{
		Object instance = instances.get(instanceId);
		if(instance instanceof XADiskJTABootstrap){
			return XADiskJTABootstrap.getInstance(instanceId, publishFileStateChangeEventsOnCommit);
		}else{
			return XADiskJCABootstrap.getInstance(instanceId, publishFileStateChangeEventsOnCommit);
		}
	}

}
