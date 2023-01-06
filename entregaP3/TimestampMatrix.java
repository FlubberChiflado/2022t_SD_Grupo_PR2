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
import java.util.Map;
import java.util.Enumeration;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

;

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
	//Constructor necesario para el metodo clone
	public TimestampMatrix() {
		super();
	}
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node){

		// Devolvemos el valor del vector del nodo recibido 
		if(node == null) {
			return null;
		}
		TimestampVector timestampVector = timestampMatrix.get(node);
		return timestampVector;
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public void updateMax(TimestampMatrix tsMatrix){
		//Recorremos las entradas de la matriz tsMatrix
		for(Map.Entry<String, TimestampVector> entry: tsMatrix.timestampMatrix.entrySet()) {
			String key = entry.getKey();

			//Recuperamos los timestampVectors de esta llave
			TimestampVector tsTimestamp = entry.getValue();
			TimestampVector thsTimestamp = this.timestampMatrix.get(key);

			//Comprobamos que el tsTimestamp no sea nulo para evitar errores
			if(tsTimestamp != null) {
				tsTimestamp.updateMax(thsTimestamp);
			}
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public void update(String node, TimestampVector tsVector){
		//Si el node ya tiene un timestamp hemos de remplazar el timestamp, si no hemos de insertar
		if(this.timestampMatrix.get(node) != null) this.timestampMatrix.replace(node, tsVector);
		else this.timestampMatrix.put(node, tsVector);
	}
	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public TimestampVector minTimestampVector(){
		TimestampVector returnVector = null;
		//Iteramos sobre los vectores que contiene esta matrix
		for(TimestampVector vector : this.timestampMatrix.values()) {

			//Si aun no tenemos vector para retornar clonamos el iterado, si no escogemos el minimo
			if (returnVector == null) {
				returnVector = vector.clone();
			} else {
				returnVector.mergeMin(vector);
			}
		}
		//Devolvemos el vector minimo
		return returnVector;
	}
	
	/**
	 * clone
	 */
	public TimestampMatrix clone(){

		//Creamos el nuevo objeto timestampmatrix
		TimestampMatrix clonedMatrix = new TimestampMatrix();

		//Para cada entrada de la matriz original creamos otra igual en la matriz copiada
		for(Map.Entry<String, TimestampVector> entry: timestampMatrix.entrySet()) {
			clonedMatrix.timestampMatrix.put(entry.getKey(), entry.getValue());
		}

		return clonedMatrix;
	}
	
	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {

		//Si obj hace referencia al mismo objeto que this obviamente devolvemos true
		if (obj == this) return true;
		//Si obj es nulo o los objetos no son de la misma clase devolvemos falso
		if(obj == null || this.getClass() != obj.getClass()) return false;

		TimestampMatrix obj2;
		obj2 = (TimestampMatrix) obj;
		return obj2.timestampMatrix.equals(this.timestampMatrix);
	}
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";

		if(this.timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=this.timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(this.timestampMatrix.get(name)!=null) {
				all += name+":   "+this.timestampMatrix.get(name)+"\n";
			}
		}
		return all;
	}
}
