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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionOriginatorSide extends TimerTask{
	private static AtomicInteger session_number = new AtomicInteger(0);

	private ServerData serverData;
	public TSAESessionOriginatorSide(ServerData serverData){
		super();
		this.serverData=serverData;
	}

	/**
	 * Implementation of the TimeStamped Anti-Entropy protocol
	 */
	public void run(){
		sessionWithN(serverData.getNumberSessions());
	}

	/**
	 * This method performs num TSAE sessions
	 * with num random servers
	 * @param num
	 */
	public void sessionWithN(int num){
		if(!SimulationData.getInstance().isConnected())
			return;
		List<Host> partnersTSAEsession= serverData.getRandomPartners(num);
		Host n;
		for(int i=0; i<partnersTSAEsession.size(); i++){
			n=partnersTSAEsession.get(i);
			sessionTSAE(n);
		}
	}

	/**
	 * This method perform a TSAE session
	 * with the partner server n
	 * @param n
	 */
	private void sessionTSAE(Host n){
		int current_session_number = session_number.incrementAndGet();
		if (n == null) return;

		//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] TSAE session");

		try {
			Socket socket = new Socket(n.getAddress(), n.getPort());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

			TimestampVector localSummary;
			TimestampMatrix localAck;
			// Añadido
			synchronized (serverData)
			{
				localSummary = this.serverData.getSummary().clone();			// Almacenamos en localSummary el timestamp clonado
				serverData.getAck().update(serverData.getId(), localSummary);	// Actualizamos la estructura de datos del Server
				localAck = serverData.getAck().clone();							// Almacenamos en localAck actual
			}
			// Fin
			// Send to partner: local's summary and ack
			Message	msg = new MessageAErequest(localSummary, localAck); // --- TSAESessionPartnerSide ejemplo Phase2
			msg.setSessionNumber(current_session_number);
			out.writeObject(msg); // --- TSAESessionPartnerSide ejemplo Phase2
			//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

			// receive operations from partner
			// Añadido
			List<MessageOperation> operations = new ArrayList<MessageOperation>();					// Creamos la variable operations para que almacene la lista de operaciones del partner
			// Fin
			msg = (Message) in.readObject(); // --- TSAESessionPartnerSide ejemplo Phase2
			//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
			while (msg.type() == MsgType.OPERATION){ // --- TSAESessionPartnerSide ejemplo Phase2
				// Añadido
				operations.add((MessageOperation) msg);								// Añadimos en la lista el msg
				// Fin
				msg = (Message) in.readObject(); // --- TSAESessionPartnerSide ejemplo Phase2
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
			}

			// receive partner's summary and ack
			if (msg.type() == MsgType.AE_REQUEST){ // --- TSAESessionPartnerSide ejemplo Phase2
				// Añadido
				MessageAErequest messageAE = (MessageAErequest) msg;				// Castea el mensaje recibido
				List<Operation> newLogs = serverData.getLog().listNewer(messageAE.getSummary());	// Y se añade a la lista de operaciones

				for (Operation op : newLogs)										// Recorremos todas los logs
				{
					out.writeObject(new MessageOperation(op));						// Enviamos el log
					//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

				}
				// Fin

				// send operations

				//...
				/* --- Implementacion oficial ---
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);
					LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);
				*/
				// send and "end of TSAE session" message
				msg = new MessageEndTSAE(); // --- TSAESessionPartnerSide ejemplo Phase2
				msg.setSessionNumber(current_session_number);
				out.writeObject(msg); // --- TSAESessionPartnerSide ejemplo Phase2
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

				// receive message to inform about the ending of the TSAE session
				msg = (Message) in.readObject(); // --- TSAESessionPartnerSide ejemplo Phase2
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
				if (msg.type() == MsgType.END_TSAE){ // --- TSAESessionPartnerSide ejemplo Phase2
					// Añadido
					synchronized (serverData) {
						for (MessageOperation operation : operations) {				// Recorremos la lista de operaciones
							if(operation.getOperation().getType() == OperationType.ADD) {
								serverData.addOp(operation);
							} else {
								serverData.removeOp(operation);
							}
						}

						serverData.getSummary().updateMax(messageAE.getSummary());	// Actualizamos summary
						serverData.getAck().updateMax(messageAE.getAck());			// Actualizamos ACK
						serverData.getLog().purgeLog(serverData.getAck());			// Eliminamos las operaciones del registro recnocidas por todos los miembros
					}
					// Fin
				}

			}
			socket.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			//LSimLogger.log(Level.FATAL, "[TSAESessionOriginatorSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}catch (IOException e) {
		}


		//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] End TSAE session");
	}
}
