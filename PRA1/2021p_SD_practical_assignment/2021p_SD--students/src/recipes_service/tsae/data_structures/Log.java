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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * Sychronized??
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op){
		//TODO Phase 1
		//Añadir la operación al log, en caso de que el timeStamp sea posterior al último registrado en este	

		Timestamp lastTime; //se usa para registrar el timestamp de la última operación
		String hostId = op.getTimestamp().getHostid(); //obtenemos el id del host que ha envíado la operación
		List<Operation> ops = log.get(hostId); //Creamos una lista con los logs que atañen al host que envía la operacion op
		
		//checkeamos si existe alguna operación previa en la lista de operaciones
		if (ops == null || ops.isEmpty()) {
			//No hay ninguna operación en el log
			lastTime = null;
		}else {
			lastTime = ops.get(ops.size() -1).getTimestamp();
			//Obetenmos el timestamp de la última operación
			//Sabemos que la última operación es la que ocupa la posición, tamaño de lista -1, 
			//ya que la primera ocupa la posición 0 
		}
		//Una vez obtenido el timeStamp de la última operación la compararemos con el timestamp de la operción actual
		//para comprobar que no sea anterior
		long diff = op.getTimestamp().compare(lastTime);
		
		if ((lastTime == null && diff == 0) || (lastTime != null && diff==1)) {
			//si el timeStamp de la operacion op es posterior al de la última anotación en el log, la añadimos
			log.get(hostId).add(op);
			return true;
		}else {
			//en el caso de que el timeStamp de la operación op no cumpla con la condición,
			//no la añadimos al log
			return false;
		}
		
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public List<Operation> listNewer(TimestampVector sum){

		// return generated automatically. Remove it when implementing your solution 
		return null;
	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		//TODO Phase 1
		//comprobamos que el objeto que hemos recibido no es null y que es una instancia de la clase Log
		if ((obj == null) || !(obj instanceof Log)) {
			//Si no es una instancia de la clase Log, devolvemos false
			return false;
		}
		Log temp = (Log) obj;
		//Tras chequear que tanto el objeto recibido como el de la clase no son null, 
		//realizamos la comprobación de que ambos obejtos sean iguales o no mediante la función equals
		if ((this.log == null) || (temp.log == null)) {
			return false;
		}else {
			return this.log.equals((temp).log);
		}

	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}
