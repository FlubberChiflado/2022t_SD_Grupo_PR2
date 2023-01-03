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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimCoordinator;
import lsim.library.api.LSimFactory;
import lsim.library.api.LSimLogger;
import lsim.library.api.LSimWorker;

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
	public synchronized void updateTimestamp(Timestamp timestamp){
		LSimLogger.log(Level.TRACE, "Updating the TimestampVectorInserting with the timestamp: "+timestamp);

		//Si la marca de tiempo es valida lo añadimos al vector de resumen
		if (timestamp != null) {
			this.timestampVector.replace(timestamp.getHostid(), timestamp);
		}
	}

	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public void updateMax(TimestampVector tsVector){
		// Accedemos al hashmap del timestamp pasado por parametros e iteramos todas sus keys
		for (Iterator<String> itString = tsVector.timestampVector.keySet().iterator(); itString.hasNext(); ){
			// Guardamos la key que esta iterando
			String str = itString.next();
			// Guardamos los dos timestamp en dos variables para compararlos posteriormente
			Timestamp tsVector1 = tsVector.timestampVector.get(str);
			Timestamp tsVector2 = timestampVector.get(str);
			// Actualizamos el correspondiente timestamp atraves de una comprobacion con un operador ternario
			updateTimestamp(tsVector1.compare(tsVector2) > 0 ? tsVector1 : tsVector2);
		}
	}

	/**
	 *
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public Timestamp getLast(String node){

		//recibimos por parametro la key del hashmap asi que retornamos su valor
		return this.timestampVector.get(node);
	}

	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public void mergeMin(TimestampVector tsVector){
		// Accedemos al hashmap del timestamp vector pasado por parametro y recorremos sus keys
		for (Iterator<String> iteratorKeys = tsVector.timestampVector.keySet().iterator(); iteratorKeys.hasNext(); ){
			// Guardamos la siguiente key del iterator
			String nextKey = iteratorKeys.next();
			// Guardamos los dos timestamps en una variable para compararlos posteriormente
			Timestamp timestamp1 = tsVector.timestampVector.get(nextKey);
			Timestamp timestamp2 = timestampVector.get(nextKey);
			// Actualizamos el timesmap dependiendo de cual es mayor y usamos un operador terneraio
			updateTimestamp(timestamp1.compare(timestamp2) < 0 ? timestamp1 : timestamp2);
		}
	}

	/**
	 * clone
	 */
	public TimestampVector clone(){
		// Declaramos e inicializamos el timestamap que sera clonado
		TimestampVector timestamp = new TimestampVector(new ArrayList<String>());
		// Recorremos las keys del hashmap de timestamp
		for (Iterator<String> iteratorTimestamp = timestampVector.keySet().iterator(); iteratorTimestamp.hasNext(); ){
			// Recogemos la siguiente key
			String keyId = iteratorTimestamp.next();
			// La añadimos al hashmap
			timestamp.timestampVector.put(keyId, timestampVector.get(keyId));
		}
		// Retornamos el timestamp clonado
		return timestamp;
	}

	/**
	 * equals
	 */
	public boolean equals(Object obj){
		if (obj == null) {
			return false;
		} else if (this == obj) {
			return true;
		} else if (!(obj instanceof TimestampVector)) {
			return false;
		}
		return false;
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
