/*
 * ! ******************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 * ******************************************************************************
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * *****************************************************************************
 */

package org.pentaho.di.trans.ael.adapters;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.engine.api.Engine;
//import org.pentaho.di.engine.api.ExecutionContext;
import org.pentaho.di.engine.api.remote.ExecutionRequest;
import org.pentaho.di.engine.api.ExecutionResult;
import org.pentaho.di.engine.api.events.PDIEvent;
import org.pentaho.di.engine.api.model.Operation;
import org.pentaho.di.engine.api.remote.*;
import org.pentaho.di.engine.api.model.Transformation;
import org.pentaho.di.engine.api.reporting.LogEntry;
import org.pentaho.di.engine.api.reporting.LogLevel;
import org.pentaho.di.engine.api.reporting.Status;
import org.pentaho.di.engine.model.ActingPrincipal;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.security.Principal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Created by fcamara on 8/24/17.
 */
public class TransWebSocketEngineAdapter extends Trans {

  public static final String ANONYMOUS_PRINCIPAL = "anonymous";
  private final Transformation transformation;
  //private final ExecutionContext executionContext;
  private final ExecutionRequest executionRequest;
  private CompletableFuture<ExecutionResult>
    executionResultFuture;
  DaemonMessagesClientEndpoint daemonMessagesClientEndpoint = new DaemonMessagesClientEndpoint();


  public static final Map<org.pentaho.di.core.logging.LogLevel, LogLevel> LEVEL_MAP = new HashMap<>();

  static {
    LEVEL_MAP.put( org.pentaho.di.core.logging.LogLevel.BASIC, LogLevel.BASIC );
    LEVEL_MAP.put( org.pentaho.di.core.logging.LogLevel.DEBUG, LogLevel.DEBUG );
    LEVEL_MAP.put( org.pentaho.di.core.logging.LogLevel.DETAILED, LogLevel.DETAILED );
    LEVEL_MAP.put( org.pentaho.di.core.logging.LogLevel.ERROR, LogLevel.ERROR );
    LEVEL_MAP.put( org.pentaho.di.core.logging.LogLevel.MINIMAL, LogLevel.MINIMAL );
    LEVEL_MAP.put( org.pentaho.di.core.logging.LogLevel.ROWLEVEL, LogLevel.TRACE );
  }

  public TransWebSocketEngineAdapter(Engine engine, TransMeta transMeta ) {
    transformation = TransMetaConverter.convert( transMeta );
    //executionContext = engine.prepare( transformation );
    //executionContext.setActingPrincipal( getActingPrincipal( transMeta ) );
    executionRequest = new ExecutionRequest( transformation );
    this.transMeta = transMeta;
  }

  @Override public void setLogLevel( org.pentaho.di.core.logging.LogLevel logLogLevel ) {
    executionRequest.setLoggingLogLevel( LEVEL_MAP.getOrDefault( logLogLevel, LogLevel.MINIMAL ) );
  }

  @Override public void killAll() {
    throw new UnsupportedOperationException( "Not yet implemented" );
  }

  @Override public void stopAll() {
    //send stop message
    daemonMessagesClientEndpoint.sendMessage(new StopMessage("User request"));
  }

  @Override public void prepareExecution( String[] arguments ) throws KettleException {
    activateParameters();
    transMeta.activateParameters();
    transMeta.setInternalKettleVariables();

    Map<String, Object> env = Arrays.stream( transMeta.listVariables() )
      .collect( toMap( Function.identity(), transMeta::getVariable ) );

    executionRequest.setEnvironment( env );

    setSteps( new ArrayList<>( opsToSteps() ) );
    wireStatusToTransListeners();

    subscribeToOpLogging();

    setReadyToStart( true );
    subscribeToTransLogging();
  }

  private void logToChannel( LogChannelInterface logChannel, LogEntry data ) {
    LogLevel logLogLevel = data.getLogLogLevel();
    switch ( logLogLevel ) {
      case ERROR:
        logChannel.logError( data.getMessage() );
        break;
      case MINIMAL:
        logChannel.logMinimal( data.getMessage() );
        break;
      case BASIC:
        logChannel.logBasic( data.getMessage() );
        break;
      case DETAILED:
        logChannel.logDetailed( data.getMessage() );
        break;
      case DEBUG:
        logChannel.logDebug( data.getMessage() );
        break;
      case TRACE:
        logChannel.logRowlevel( data.getMessage() );
        break;
    }
  }

  private void subscribeToOpLogging() {
    transformation.getOperations().stream().forEach( operation -> {
      daemonMessagesClientEndpoint.addMessageHandler(DaemonMessagesClientEndpoint.HandlerType.OPERATION_LOG, operation.getId(), new DaemonMessagesClientEndpoint.MessageHandler() {
        @Override
        public void handleMessage(Message message) {
          PDIEvent<Operation, LogEntry> event = (PDIEvent<Operation, LogEntry>)message;
          LogEntry logEntry = event.getData();
          StepInterface stepInterface = findStepInterface(operation.getId(), 0);
          if (stepInterface != null) {
            LogChannelInterface logChannel = stepInterface.getLogChannel();
            logToChannel(logChannel, logEntry);
          } else {
            // Could not find step, log at transformation level instead
            logToChannel(getLogChannel(), logEntry);
          }
        }
      });
    });
  }

  private void subscribeToTransLogging() {
      daemonMessagesClientEndpoint.addMessageHandler(DaemonMessagesClientEndpoint.HandlerType.TRANSFORMATION_LOG, null, new DaemonMessagesClientEndpoint.MessageHandler() {
        @Override
        public void handleMessage(Message message) {
          PDIEvent<Transformation, LogEntry> event = (PDIEvent<Transformation, LogEntry>)message;
          LogEntry data = event.getData();
          logToChannel( getLogChannel(), data );
        }
      });
  }

  private void wireStatusToTransListeners() {
    daemonMessagesClientEndpoint.addMessageHandler(DaemonMessagesClientEndpoint.HandlerType.TRANSFORMATION_STATUS, null, new DaemonMessagesClientEndpoint.MessageHandler() {
      @Override
      public void handleMessage(Message message) {
        PDIEvent<Transformation, Status> transStatusEvent = (PDIEvent<Transformation, Status>) message;
        addStepPerformanceSnapShot();
        getTransListeners().forEach( l -> {
          try {
            switch ( transStatusEvent.getData() ) {
              case RUNNING:
                l.transStarted( TransWebSocketEngineAdapter.this );
                l.transActive( TransWebSocketEngineAdapter.this );
                break;
              case PAUSED:
                break;
              case STOPPED:
                break;
              case FAILED:
              case FINISHED:
                l.transFinished( TransWebSocketEngineAdapter.this );
                setFinished( true );
                break;
            }
          } catch ( KettleException e ) {
            throw new RuntimeException( e );
          }
        });
      }
    });

    daemonMessagesClientEndpoint.addMessageHandler(DaemonMessagesClientEndpoint.HandlerType.ERROR, null, new DaemonMessagesClientEndpoint.MessageHandler() {
      @Override
      public void handleMessage(Message message) {
        Throwable throwable = ((ErrorMessage) message).getThrowable();

        getLogChannel().logError( "Error Executing Transformation", throwable );
        setFinished( true );
        // emit error on all steps
        getSteps().stream().map( stepMetaDataCombi -> stepMetaDataCombi.step ).forEach( step -> {
          step.setStopped( true );
          step.setRunning( false );
        } );
        getTransListeners().forEach( l -> {
          try {
            l.transFinished( TransWebSocketEngineAdapter.this );
          } catch ( KettleException e ) {
            getLogChannel().logError( "Error notifying trans listener", e );
          }
        } );
      }
    });

    daemonMessagesClientEndpoint.addMessageHandler(DaemonMessagesClientEndpoint.HandlerType.COMPLETE, null, new DaemonMessagesClientEndpoint.MessageHandler() {
      @Override
      public void handleMessage(Message message) {
        setFinished( true );
        getTransListeners().forEach( l -> {
          try {
            l.transFinished( TransWebSocketEngineAdapter.this );
          } catch ( KettleException e ) {
            getLogChannel().logError( "Error notifying trans listener", e );
          }
        } );
      }
    });
  }

  private Collection<StepMetaDataCombi> opsToSteps() {
    Map<Operation, StepMetaDataCombi> operationToCombi = transformation.getOperations().stream()
      .collect( toMap( Function.identity(),
        op -> {
          StepMetaDataCombi combi = new StepMetaDataCombi();
          combi.stepMeta = StepMeta.fromXml( (String) op.getConfig().get( TransMetaConverter.STEP_META_CONF_KEY ) );
          combi.data = new StepDataInterfaceEngineWebSocketAdapter( op, daemonMessagesClientEndpoint );
          combi.step = new StepInterfaceEngineWebSocketAdapter( op, daemonMessagesClientEndpoint, combi.stepMeta, transMeta,
            combi.data, this );
          combi.meta = combi.stepMeta.getStepMetaInterface();
          combi.stepname = combi.stepMeta.getName();
          return combi;
        } ) );
    return operationToCombi.values();
  }

  @Override public void startThreads() throws KettleException {
    daemonMessagesClientEndpoint.sendMessage(executionRequest);
  }

  @Override public void waitUntilFinished() {
    try {
      ExecutionResult result = executionResultFuture.get();
    } catch ( InterruptedException e ) {
      throw new RuntimeException( "Waiting for transformation to be finished interrupted!", e );
    } catch ( ExecutionException e ) {
      throw new RuntimeException( "Error executing Transformation or waiting for it to stop", e );
    }
  }


  // ======================== May want to implement ================================= //


  @Override public RowProducer addRowProducer( String stepname, int copynr ) throws KettleException {
    throw new UnsupportedOperationException( "Not yet implemented" );
  }

  private Principal getActingPrincipal( TransMeta transMeta ) {
    if ( transMeta.getRepository() == null || transMeta.getRepository().getUserInfo() == null ) {
      return new ActingPrincipal( ANONYMOUS_PRINCIPAL );
    }
    return new ActingPrincipal( transMeta.getRepository().getUserInfo().getName() );
  }
}
