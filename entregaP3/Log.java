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

import lsim.library.api.LSimFactory;
import lsim.library.api.LSimWorker;
import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;


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
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op){
		//Almacenamos en operation log la lista de operaciones y obtenemos el host id
		List<Operation> operationLog = log.get(op.getTimestamp().getHostid());
		//Si el tamaño de la lista es mayor a 0...
		if (operationLog.size() > 0){
			//Almacenamos en endLog la ultima lista
			Operation endLog = operationLog.get(operationLog.size() - 1);
			//Almacenamos en dif la marca de tiempo de op con la endLog
			long dif = endLog.getTimestamp().compare(op.getTimestamp());
			//Si la marca de tiempo no es correcta entonces devuelve false
			if (dif >= 0){
				return false;
			}
		}
		//Si la marca de tiempo es correcta se añade la operacion al log
		operationLog.add(op);
		log.put(op.getTimestamp().getHostid(), operationLog);
		return true;
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
		List<Operation> newOperations = new Vector<Operation>();							// Almacenamos las nuevas operaciones
		List<String> hosts = new Vector<String>(this.log.keySet());						// Almacenaremos todos los hosts del registo

		for (Iterator<String> iterator = hosts.iterator(); iterator.hasNext(); ){	// Recorremos todas las claves
			String node = iterator.next();											// Almacenamos en node el siguiente nodo
			List<Operation> operations = new Vector<Operation>(this.log.get(node));			// Almacenamos en operations la lista de operaciones del nodo
			Timestamp timestamp = sum.getLast(node);								// Almacenamos en timestamp el valor de la key del hashmap

			for (Operation op : operations) {										// Recorremos todas las operaciones de los hosts
				if (op.getTimestamp().compare(timestamp) > 0)						// Si el timestamps es mayor
					newOperations.add(op);										// Añadimos la operacion
			}
		}
		return newOperations;														// Devolvemos la lista con todas las nuevas operaciones
	}

	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary.
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
		//Conseguimos el minimo timestamp
		TimestampVector min = ack.minTimestampVector();
		Timestamp last = null;
		//Recorremos todas las entradas del log

		for(String key : this.log.keySet()) {
			//Para cada entrada obtenemos el ultimo timestamp
			last = min.getLast(key);
			//Miramos que la operacion exista
			if(last != null) {
				//Obtenemos la lista de esa entrada
				for (Iterator<Operation> op = log.get(key).iterator(); op.hasNext(); ) {
					//Miramos Que sea anterior a la ultima
					if (op.next().getTimestamp().compare(last) <= 0) {
						op.remove(); //Removemos la operacion
					}
				}
			}
		}
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		//Creamos log2 donde se almacenara el segundo log para debugear
		Log log2 = (Log) obj;

		//Si el objeto existe o si los dos logs son iguales devuelve verdadero
		if (this == obj || this.log == log2.log) {
			return true;
		}
		//Si el objeto, o el primero o el segundo log o el objeto no es de la misma clase
		//devolvera falso
		if (obj == null || this.log == null || log2.log == null || !(obj instanceof Log)) {
			return false;
		}

		//Si los logs no coinciden devuelve el resutaldo de la comparacion
		return this.log.equals(log2.log);
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
