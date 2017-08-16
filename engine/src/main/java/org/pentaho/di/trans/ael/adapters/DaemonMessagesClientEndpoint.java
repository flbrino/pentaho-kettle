package org.pentaho.di.trans.ael.adapters;

import org.pentaho.di.engine.api.remote.*;
import org.pentaho.di.engine.api.events.*;
import org.pentaho.di.engine.api.model.*;
import org.pentaho.di.engine.api.reporting.Status;
import javax.websocket.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;

@ClientEndpoint(encoders=MessageEncoder.class, decoders=MessageDecoder.class)
public class DaemonMessagesClientEndpoint {
  Session userSession = null;

  private Hashtable<SearchKeys, MessageHandler> handlers = new Hashtable<>();
  private MessageHandler transformationEventHandler;
  private MessageHandler operationEventHandler;
  private MessageHandler metricMessage;
  private MessageHandler stopMessage;
  private MessageHandler operationLogEntry;
  private MessageHandler errorEventHandler;
  private MessageHandler completeEventHandler;


  public DaemonMessagesClientEndpoint() {
    try {
      WebSocketContainer container = ContainerProvider.getWebSocketContainer();
      container.connectToServer(this, new URI("ws://localhost:8080/daemonMock"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Callback hook for Connection open events.
   *
   * @param userSession the userSession which is opened.
   */
  @OnOpen
  public void onOpen(Session userSession) {
    System.out.println("opening websocket");
    this.userSession = userSession;
  }

  /**
   * Callback hook for Connection close events.
   *
   * @param userSession the userSession which is getting closed.
   * @param reason the reason for connection close
   */
  @OnClose
  public void onClose(Session userSession, CloseReason reason) {
    System.out.println("closing websocket");
    this.userSession = null;
  }

  /**
   * Callback hook for Message Events. This method will be invoked when a client send a message.
   *
   * @param message The text message
   */
  @OnMessage
  public void onMessage(Message message, Session session) {
    if( message instanceof PDIEvent ) {
      PDIEvent pdiEvent = (PDIEvent) message;

      if( "Transformation".equals( pdiEvent.getSource().getClass().getSimpleName() ) && "LogEntry".equals( pdiEvent.getData().getClass().getSimpleName() )  ) {
        handleMessage(HandlerType.TRANSFORMATION_LOG, null, message);
      }
      else if( "Transformation".equals( pdiEvent.getSource().getClass().getSimpleName() ) && "Status".equals( pdiEvent.getData().getClass().getSimpleName() )  ) {
        handleMessage(HandlerType.TRANSFORMATION_STATUS, null, message);
      }
      else if ("Operation".equals( pdiEvent.getSource().getClass().getSimpleName() ) && "Status".equals( pdiEvent.getData().getClass().getSimpleName() ) ) {
        handleMessage(HandlerType.OPERATION_STATUS, pdiEvent.getSource().getId(), message);
      }
      else if ("Operation".equals( pdiEvent.getSource().getClass().getSimpleName() ) && "Rows".equals( pdiEvent.getData().getClass().getSimpleName() ) ) {
        handleMessage(HandlerType.ROWS, pdiEvent.getSource().getId(), message);
      }
      else if ("Operation".equals( pdiEvent.getSource().getClass().getSimpleName() ) && "Metrics".equals( pdiEvent.getData().getClass().getSimpleName() ) ) {
        handleMessage(HandlerType.METRICS, pdiEvent.getSource().getId(), message);
      }
      else if ("Operation".equals( pdiEvent.getSource().getClass().getSimpleName() ) && "LogEntry".equals( pdiEvent.getData().getClass().getSimpleName() ) ) {
        handleMessage(HandlerType.OPERATION_LOG, pdiEvent.getSource().getId(), message);
      }
    }
  }

  private void handleMessage(HandlerType handlerType, String objectId, Message message) {
    handlers.entrySet().stream().filter(
        p -> handlerType == p.getKey().getHandlerType() &&
          ((objectId == null && p.getKey().getObjectID() == null) ||
            (objectId != null && objectId.equals(p.getKey().getObjectID())))
        ).forEach(item -> item.getValue().handleMessage(message));
  }

  public void addMessageHandler(HandlerType handlerType, String operationId, MessageHandler messageHandler) {
    handlers.put(new SearchKeys(handlerType, operationId), messageHandler);
  }

  /**
   * Send a message.
   *
   * @param message
   */
  public void sendMessage(ExecutionRequest request) {
    try {
      this.userSession.getBasicRemote().sendObject(request);
    }catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void sendMessage(StopMessage stopMessage) {
    try {
      this.userSession.getBasicRemote().sendObject( stopMessage );
    }catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Message handler.
   *
   */
  public static interface MessageHandler {
    public void handleMessage(Message message);
  }

  class SearchKeys{
    HandlerType handlerType;
    String objectID;

    SearchKeys(HandlerType handlerType, String objectID){
      this.handlerType = handlerType;
      this.objectID = objectID;
    }

    SearchKeys(HandlerType handlerType){
      this.handlerType = handlerType;
      this.objectID = null;
    }

    public HandlerType getHandlerType() {
      return handlerType;
    }

    public String getObjectID() {
      return objectID;
    }
  }

  public enum HandlerType {
    TRANSFORMATION_STATUS,
    OPERATION_STATUS,
    TRANSFORMATION_LOG,
    OPERATION_LOG,
    COMPLETE,
    ERROR,
    STOP,
    METRICS,
    ROWS
  }

}
