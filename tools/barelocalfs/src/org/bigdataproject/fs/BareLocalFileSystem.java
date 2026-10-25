package org.bigdataproject.fs;

import org.apache.hadoop.fs.LocalFileSystem;

/**
 * Drop-in replacement for Hadoop's LocalFileSystem that works on Windows
 * without winutils.exe or hadoop.dll.
 *
 * It is the checksummed wrapper around {@link BareRawLocalFileSystem}, which
 * holds the actual workarounds. It must extend LocalFileSystem rather than
 * RawLocalFileSystem, because FileSystem.getLocal() — reached by, among others,
 * SparkContext.textFile() — casts fs.file.impl to LocalFileSystem.
 *
 * Register it on the SparkSession with:
 *     spark.hadoop.fs.file.impl = org.bigdataproject.fs.BareLocalFileSystem
 */
public class BareLocalFileSystem extends LocalFileSystem {

    public BareLocalFileSystem() {
        super(new BareRawLocalFileSystem());
    }
}
