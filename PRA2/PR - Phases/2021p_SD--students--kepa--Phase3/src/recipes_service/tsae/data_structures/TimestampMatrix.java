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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{
	
	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	
	//Phase3
	//Se usa para crear un nuevo objeto sin datos, para en el clonado luego poder rellenarlo con datos
	public TimestampMatrix() {
	}
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node){
		//Phase 3
		//Devuelve el valor del node
		return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public synchronized void updateMax(TimestampMatrix tsMatrix){
		//Phase 3
		// Recorremos el hashmap ya que necesitamos ir comparando los timestampVector de cada key
		for (Map.Entry<String, TimestampVector> keys : tsMatrix.timestampMatrix.entrySet()) {
			// Obtenemos la clave y declaramos los dos timestamp para actualizar el maximo
			String key = keys.getKey();
			// Guardamos los dos timestampVector en dos variables para compararlos
			TimestampVector tsv1 = keys.getValue();
			TimestampVector tsv2 = this.timestampMatrix.get(key);		
			// Revisamos que no sea null antes de actualizar, ya 
			if (tsv2 != null) {
				// Finalmente actualizamos
				tsv2.updateMax(tsv1);
			}
		}
	}

	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public synchronized void update(String node, TimestampVector tsVector) {
		// Evaluamos el caso en el que tengamos que actualizar o reemplazar
		if (this.timestampMatrix.get(node) != null) {
			this.timestampMatrix.replace(node, tsVector);
		} else {
			this.timestampMatrix.put(node, tsVector);
		}
	}

	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public synchronized TimestampVector minTimestampVector(){
		//Phase3
		TimestampVector tsv = null;
		// Recorremos el hashmap ya que necesitamos el timestampVector
		for (TimestampVector eachTimestampVector : this.timestampMatrix.values()) {
			// Si el timestamp es null lo clonamos y en caso contrario llamamos al metodo merge
			if (tsv == null)
				tsv = eachTimestampVector.clone();
			else
				tsv.mergeMin(eachTimestampVector);
		}
		// Devuelve el timestampVector
		return tsv;
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampMatrix clone(){
		//Phase 3
		TimestampMatrix tsCloner = new TimestampMatrix();
		// Iteramos las keys del hashmap y vamos construyendo el clon con los datos de este
		for (Map.Entry<String, TimestampVector> data : timestampMatrix.entrySet()) {
			tsCloner.timestampMatrix.put(data.getKey(), data.getValue().clone());
		}

		return tsCloner;
	}
	
	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		// Comparamos el obj que exista y devolvemos true si existe
		if (this == obj)
			return true;
		// Comparamos el obj que exista y devolvemos false si no existe
		if (obj == null)
			return false;
		// Comparamos que el obj sea de la clase que queremos, sino devolvemos false
		if (getClass() != obj.getClass())
			return false;
		// Si los dos Log son iguales devolvemos el valor
		TimestampMatrix newLog = (TimestampMatrix) obj;
		return newLog.timestampMatrix.equals(timestampMatrix);
	}
	
	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
