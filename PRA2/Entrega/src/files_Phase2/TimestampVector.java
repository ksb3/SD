/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();
	
	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}

	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public void updateTimestamp(Timestamp timestamp){
		//TODO Phase 1
		//Añadimos la marca de tiempo recibida al vector de marcas de tiempo, si esta no es null
		if (timestamp != null) {
			this.timestampVector.put(timestamp.getHostid(),timestamp);
		}
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector){
		//TODO Phase2
		// Recorremos el hashmap con un iterator
		for (Iterator<String> itString = tsVector.timestampVector.keySet().iterator(); itString.hasNext(); ){
			String str = itString.next();
			// Guardamos los dos timestamp en dos variables para compararlos
			Timestamp ts1 = tsVector.timestampVector.get(str);
			Timestamp ts2 = timestampVector.get(str);
			// Actualizamos el timestamp atraves de una comparación para actualizarlo con el mayor
			updateTimestamp(ts1.compare(ts2) > 0 ? ts1 : ts2);
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public synchronized Timestamp getLast(String node){
		//TODO Phase2
		//Devuelve el valor del node
		return this.timestampVector.get(node);
	}
	
	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public synchronized void mergeMin(TimestampVector tsVector){
		//TODO Phase2
		// Recorremos el hashmap con un iterator
		for (Iterator<String> iteratorKeys = tsVector.timestampVector.keySet().iterator(); iteratorKeys.hasNext(); ){
			// Guardamos la siguiente key del iterator
			String nextKey = iteratorKeys.next();
			// Guardamos los dos timestamps en una variable para compararlos posteriormente
			Timestamp ts1 = tsVector.timestampVector.get(nextKey);
			Timestamp ts2 = timestampVector.get(nextKey);
			// Actualizamos el timestamp atraves de una comparación para actualizarlo con el menor
			updateTimestamp(ts1.compare(ts2) < 0 ? ts1 : ts2);
		}
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampVector clone(){
		//TODO Phase2
		TimestampVector timestamp = new TimestampVector(new ArrayList<String>());
		// Recorremos el hashmap con un iterator
		for (Iterator<String> iteratorTimestamp = timestampVector.keySet().iterator(); iteratorTimestamp.hasNext(); ){
			String keyId = iteratorTimestamp.next();
			// Añadimos la keyId al hashmap
			timestamp.timestampVector.put(keyId, timestampVector.get(keyId));
		}
		//Devuelve el timestamp clonado
		return timestamp;
	}
	
	/**
	 * equals
	 */
	public boolean equals(TimestampVector param){
		//TODO Phase 1
		//Comprobamos si ambos vectores de marcas de tiempo son iguales
		if (this.timestampVector == (param.timestampVector)) {
			return true;
		}else if (this.timestampVector == null || param.timestampVector == null){
			//comprueba que ninguna de las dos marcas de tiempo sean null, en caso de que alguna lo sea devuelve null
			return false;
		}else {
			//devuelve el valor de la función equals
			return this.timestampVector.equals(param.timestampVector);
		}
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
		}
		return all;
	}
}
