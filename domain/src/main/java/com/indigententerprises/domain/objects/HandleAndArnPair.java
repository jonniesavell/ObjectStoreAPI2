package com.indigententerprises.domain.objects;

public class HandleAndArnPair {
    public final Handle handle;
    public final String arn;

    public HandleAndArnPair(
            final Handle handle,
            final String arn
    ) {
        this.handle = handle;
        this.arn = arn;
    }
}
