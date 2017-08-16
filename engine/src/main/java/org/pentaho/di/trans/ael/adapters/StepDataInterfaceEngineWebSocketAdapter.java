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

import org.pentaho.di.engine.api.ExecutionContext;
import org.pentaho.di.engine.api.events.PDIEvent;
import org.pentaho.di.engine.api.model.Operation;
import org.pentaho.di.engine.api.remote.Message;
import org.pentaho.di.engine.api.reporting.Status;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;

import static org.pentaho.di.engine.api.reporting.Status.*;
import static org.pentaho.di.trans.step.BaseStepData.StepExecutionStatus.*;

/**
 * Maps AEL Status events to corresponding step state.
 */
public class StepDataInterfaceEngineWebSocketAdapter implements StepDataInterface {

  private AtomicReference<BaseStepData.StepExecutionStatus> stepExecutionStatus =
    new AtomicReference<>( STATUS_INIT );

   StepDataInterfaceEngineWebSocketAdapter(Operation op, DaemonMessagesClientEndpoint daemonMessagesClientEndpoint ) {
    daemonMessagesClientEndpoint.addMessageHandler(DaemonMessagesClientEndpoint.HandlerType.OPERATION_STATUS, op.getId(), new DaemonMessagesClientEndpoint.MessageHandler() {
      @Override
      public void handleMessage(Message message) {
        PDIEvent<Operation, Status> data = (PDIEvent<Operation, Status>)message;
        switch ( data.getData() ) {
          case FINISHED:
            stepExecutionStatus.set( STATUS_FINISHED );
            break;
          case FAILED:
          case STOPPED:
            stepExecutionStatus.set( STATUS_STOPPED );
            break;
          case PAUSED:
            stepExecutionStatus.set( STATUS_PAUSED );
            break;
          case RUNNING:
            stepExecutionStatus.set( STATUS_RUNNING );
            break;
        }
      }
    });
  }


  @Override public void setStatus( BaseStepData.StepExecutionStatus stepExecutionStatus ) {
    this.stepExecutionStatus.set( stepExecutionStatus ); }

  @Override public BaseStepData.StepExecutionStatus getStatus() {
    return stepExecutionStatus.get();
  }

  @Override public boolean isEmpty() {
    return stepExecutionStatus.get() == STATUS_EMPTY;
  }

  @Override public boolean isInitialising() {
    return stepExecutionStatus.get() == STATUS_INIT;
  }

  @Override public boolean isRunning() {
    return stepExecutionStatus.get() == STATUS_RUNNING;
  }

  @Override public boolean isIdle() {
    return stepExecutionStatus.get() == STATUS_IDLE;
  }

  @Override public boolean isFinished() {
    return stepExecutionStatus.get() == STATUS_FINISHED;
  }

  @Override public boolean isDisposed() {
    return stepExecutionStatus.get() == STATUS_DISPOSED;
  }
}
