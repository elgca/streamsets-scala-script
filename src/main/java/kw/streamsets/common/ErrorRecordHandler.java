package kw.streamsets.common;


import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import java.util.List;

public interface ErrorRecordHandler {
    void onError(ErrorCode var1, Object... var2) throws StageException;

    void onError(OnRecordErrorException var1) throws StageException;

    void onError(List<Record> var1, StageException var2) throws StageException;
}
