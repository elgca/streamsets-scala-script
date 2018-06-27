package kw.streamsets.common;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultErrorRecordHandler implements ErrorRecordHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultErrorRecordHandler.class);
    private final ToErrorContext toError;
    private final Context context;
    private final OnRecordError onRecordError;

    public DefaultErrorRecordHandler(OnRecordError onRecordError, Context context, ToErrorContext toError) {
        this.context = context;
        this.toError = toError;
        this.onRecordError = onRecordError;
    }

    public DefaultErrorRecordHandler(Context context, ToErrorContext toError) {
        this(context.getOnErrorRecord(), context, toError);
    }

    public DefaultErrorRecordHandler(com.streamsets.pipeline.api.Source.Context context) {
        this(context, context);
    }

    public DefaultErrorRecordHandler(com.streamsets.pipeline.api.Processor.Context context) {
        this(context, context);
    }

    public DefaultErrorRecordHandler(com.streamsets.pipeline.api.Target.Context context) {
        this(context, context);
    }

    public void onError(ErrorCode errorCode, Object... params) throws StageException {
        this.validateGetOnErrorRecord((Exception) null, errorCode, params);
        switch (this.onRecordError) {
            case TO_ERROR:
                this.context.reportError(errorCode, params);
            case DISCARD:
                return;
            case STOP_PIPELINE:
                throw new StageException(errorCode, params);
            default:
                throw new IllegalStateException(Utils.format("Unknown OnError value '{}'", new Object[]{this.onRecordError}));
        }
    }

    public void onError(OnRecordErrorException error) throws StageException {
        this.validateGetOnErrorRecord(error, (ErrorCode) null, (Object[]) null);
        switch (this.onRecordError) {
            case TO_ERROR:
                this.toError.toError(error.getRecord(), error);
            case DISCARD:
                return;
            case STOP_PIPELINE:
                throw error;
            default:
                throw new IllegalStateException(Utils.format("Unknown OnError value '{}'", new Object[]{this.onRecordError}), error);
        }
    }

    public void onError(List<Record> batch, StageException error) throws StageException {
        this.validateGetOnErrorRecord(error, (ErrorCode) null, (Object[]) null);
        switch (this.onRecordError) {
            case TO_ERROR:
                Iterator var3 = batch.iterator();

                while (var3.hasNext()) {
                    Record record = (Record) var3.next();
                    this.toError.toError(record, error);
                }
            case DISCARD:
                return;
            case STOP_PIPELINE:
                throw error;
            default:
                throw new IllegalStateException(Utils.format("Unknown OnError value '{}'", new Object[]{this.onRecordError}), error);
        }
    }

    private void validateGetOnErrorRecord(Exception ex, ErrorCode errorCode, Object... params) {
        if (this.onRecordError == null) {
            if (ex != null) {
                LOG.error("Can't propagate exception to error stream", ex);
            }

            if (errorCode != null) {
                LOG.error("Can't propagate error to error stream: {} with params {}", errorCode, params);
            }

            throw new IllegalStateException(Utils.format("Component {} doesn't have configured error record action.", new Object[]{this.context.getStageInfo().getInstanceName()}));
        }
    }
}
