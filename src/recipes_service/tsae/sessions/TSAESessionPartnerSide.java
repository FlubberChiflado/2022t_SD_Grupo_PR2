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

package recipes_service.tsae.sessions;


import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread{
	
	private Socket socket = null;
	private ServerData serverData = null;
	
	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {

		Message msg = null;

		int current_session_number = -1;
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			// receive request from originator and update local state
			// receive originator's summary and ack
			msg = (Message) in.readObject(); // --- TSAESessionPartnerSide ejemplo Phase2
			current_session_number = msg.getSessionNumber();
			LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] TSAE session");
			LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
			if (msg.type() == MsgType.AE_REQUEST){ // --- TSAESessionPartnerSide ejemplo Phase2
				// Añadido
				MessageAErequest messageAE = (MessageAErequest) msg;				// Castea el mensaje recibido
				// Fin
				TimestampVector localSummary = null;
				TimestampMatrix localAck = null;

				// Añadido
				synchronized (serverData) {
					localSummary = serverData.getSummary().clone();									// Almacenamos en localSummary el timestamp clonado
					serverData.getAck().update(serverData.getId(), localSummary);					// Actualizamos la estructura de datos del Server
					localAck = serverData.getAck().clone();											// Almacenamos en localAck actual
				}

				List<Operation> newLogs = serverData.getLog().listNewer(messageAE.getSummary()); 	// Obtenemos los logs
				for (Operation op : newLogs) {														// Recorremos todos los logs
					out.writeObject(new MessageOperation(op));										// Enviamos el log
				}
				// Fin
	            // send operations
					// ...
				/*  --- Implementacion oficial ---
					out.writeObject(msg); // --- TSAESessionPartnerSide ejemplo Phase2
					msg.setSessionNumber(current_session_number);
					LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);
				*/

				// send to originator: local's summary and ack
				msg = new MessageAErequest(localSummary, localAck); // TSAESessionPartnerSide ejemplo Phase2
				msg.setSessionNumber(current_session_number);
	 	        out.writeObject(msg); // --- TSAESessionPartnerSide ejemplo Phase2
				LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);

				// Añadido
				List<MessageOperation> operations = new ArrayList<MessageOperation>();								// Creamos la variable operations para que almacene la lista de operaciones del partner
				// Fin

	            // receive operations
				msg = (Message) in.readObject(); // --- TSAESessionPartnerSide ejemplo Phase2
				LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				while (msg.type() == MsgType.OPERATION){ // --- TSAESessionPartnerSide ejemplo Phase2
					// Añadido
					operations.add((MessageOperation) msg);											// Añadimos en la lista el msg
					// Fin
					msg = (Message) in.readObject(); // --- TSAESessionPartnerSide ejemplo Phase2
					LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				}
				
				// receive message to inform about the ending of the TSAE session
				if (msg.type() == MsgType.END_TSAE){ // --- TSAESessionPartnerSide ejemplo Phase2
					// send and "end of TSAE session" message
					msg = new MessageEndTSAE(); // --- TSAESessionPartnerSide ejemplo Phase2
					msg.setSessionNumber(current_session_number);
		            out.writeObject(msg); // --- TSAESessionPartnerSide ejemplo Phase2
					LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);

					// Añadido
					synchronized (serverData) {
						// Recorremos la lista de operaciones
						for (MessageOperation operation : operations) {								// Recorremos la lista de operaciones
							serverData.addRemoveOperation(operation);
						}

						serverData.getSummary().updateMax(messageAE.getSummary());					// Actualizamos summary
						serverData.getAck().updateMax(messageAE.getAck());							// Actualizamos ACK
						serverData.getLog().purgeLog(serverData.getAck());							// Eliminamos las operaciones del registro recnocidas por todos los miembros
					}
					// Fin

				}
				
			}
			socket.close();		
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			LSimLogger.log(Level.FATAL, "[TSAESessionPartnerSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
	    }
		
		LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] End TSAE session");
	}
}
