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
	public synchronized boolean add (Operation op){
		Operation operation = null;
		String hostId = op.getTimestamp().getHostid();
		List<Operation> listOperations = log.get(hostId);
		
		// Revisamos que no este vacio
		if( !listOperations.isEmpty() ) {
			// Obtenemos longitud de la lista
			operation = listOperations.get(listOperations.size()-1);
		}
		// Revisamos si no existe o el timestamp es null y lo añadimos
		if( operation == null || operation.getTimestamp() == null ) {
			log.get(hostId).add(op);
			return true;
			
		} else if( op.getTimestamp().compare(operation.getTimestamp()) == 1 ) {
			log.get(hostId).add(op);
			return true;
		}
		// Cualquier otro caso no se añadira y devolveremos false
		return false;
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum){
		//TODO Phase2
		List<Operation> newerListOp = new Vector<Operation>();
		List<String> participants = new Vector<String>(this.log.keySet());
		
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			// necesitamos el siguiente nodo para obtener las operaciones de los participantes
			String node = it.next();
			List<Operation> operations = new Vector<Operation>(this.log.get(node));
			Timestamp timestampToCompare = sum.getLast(node);
			// Recorremos las operaciones que obtenemos del nodo
			for (Iterator<Operation> opIteration = operations.iterator(); opIteration.hasNext(); ) {
				Operation op = opIteration.next();
				if (op.getTimestamp().compare(timestampToCompare) > 0) {
					// Comparamos los Timestamp y lo añadimos a la lista en caso de que sea mayor que el que tenemos por parametro
					newerListOp.add(op);
				}
			}
		}

		return newerListOp;
	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public synchronized void purgeLog(TimestampMatrix ack) {
		//Phase 3
		List<String> keys = new Vector<String>(this.log.keySet());
		TimestampVector minTimestamp = ack.minTimestampVector();
		
		// Usamos el Iterator para recorrer las key del log
		for (Iterator<String> iter = keys.iterator(); iter.hasNext(); ){
			// necesitamos el siguiente key para obtener las operaciones
			String key = iter.next();
			// Recorremos las operaciones que obtenemos mediante el key y un iterator
			for (Iterator<Operation> operationIterator = log.get(key).iterator(); operationIterator.hasNext();) {
				// Comprobamos que no sea null o que la comparación con el menor timestamp sea menor que 0
				// Es decir que ya haya sido recivido por los demas nodos y que podamos borrarlo
				if (minTimestamp.getLast(key) != null && operationIterator.next().getTimestamp().compare(minTimestamp.getLast(key)) <= 0) {
					// Borramos la operación
					operationIterator.remove();
				}
			}
		}
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		// Se compara el obj que exista y devolvemos true si existe
		if (this == obj)
			return true;
		// Se compara el obj que exista y devolvemos false si no existe
		if (obj == null)
			return false;
		// Comparamos que el obj sea de la clase que queremos, sino devolvemos false
		if (getClass() != obj.getClass())
			return false;
		// Si los dos Log son iguales devolvemos el valor
		Log newLog = (Log) obj;
		return newLog.log.equals(log);
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
