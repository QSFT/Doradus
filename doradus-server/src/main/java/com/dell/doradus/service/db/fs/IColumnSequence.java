package com.dell.doradus.service.db.fs;

import com.dell.doradus.service.db.Sequence;

public interface IColumnSequence extends Sequence<FsColumn> {
    public FsColumn next();
    public boolean isRowDeleted();
}
