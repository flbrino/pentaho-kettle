package org.pentaho.di.engine.api.remote;

/**
 * Close Message
 * <p>
 * Since the CloseReason is not serializable collect the necessary parts and reconstruct the object when needed.
 * <p>
 * NOTE: There was an attempt to extend CloseReason and implement Message to make it serializable; but this did not work.
 * <p>
 * TODO Add CloseReason if it is ok to add references to the javax.websocket-api depencency
 * Created by ccaspanello on 7/25/17.
 */
public class ErrorMessage implements Message {

    private static final long serialVersionUID = 8842623444691045346L;
    private Throwable throwable;

    public ErrorMessage(Throwable throwable) {
        //this.closeCode = closeCode;
        this.throwable = throwable;
    }

    public Throwable getThrowable() {
        return throwable;
    }

}
