package org.bigdataproject.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A raw local Hadoop FileSystem that does not need winutils.exe or hadoop.dll.
 *
 * On Windows, Hadoop's RawLocalFileSystem implements POSIX file permissions by
 * shelling out to winutils.exe. When that binary is absent, every write fails
 * with "HADOOP_HOME and hadoop.home.dir are unset", which is why Spark cannot
 * save an MLlib model on a plain Windows machine.
 *
 * Permissions are meaningless for this project's purpose (writing a model into
 * a local folder), so this subclass stubs out the three operations that reach
 * for winutils:
 *
 *   - setPermission / setOwner  -> no-ops
 *   - getFileStatus             -> returns a plain FileStatus carrying fixed
 *                                  permissions, so nothing ever calls
 *                                  loadPermissionInfo() lazily later on
 *   - listStatus                -> lists with java.io instead of FileUtil,
 *                                  which would call NativeIO$Windows.access0
 *
 * This is the raw filesystem. Spark registers the checksummed wrapper,
 * {@link BareLocalFileSystem}, as fs.file.impl.
 */
public class BareRawLocalFileSystem extends RawLocalFileSystem {

    /** Permissions reported for every file and directory. */
    private static final FsPermission FIXED = FsPermission.createImmutable((short) 0777);

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        // No-op: avoids Shell.getWinUtilsFile().
    }

    @Override
    public void setOwner(Path path, String username, String groupname) throws IOException {
        // No-op: avoids Shell.getWinUtilsFile().
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        FileStatus status = super.getFileStatus(path);
        if (status == null) {
            throw new FileNotFoundException("No such file or directory: " + path);
        }
        // Rebuild as a plain FileStatus. The instance returned by the superclass
        // resolves its permissions lazily through winutils on first access.
        return new FileStatus(
                status.getLen(),
                status.isDirectory(),
                status.getReplication(),
                status.getBlockSize(),
                status.getModificationTime(),
                status.getAccessTime(),
                FIXED,
                "hadoop",
                "hadoop",
                null,
                status.getPath());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        // Not delegating to super: RawLocalFileSystem.listStatus filters entries
        // through FileUtil.canRead(), which calls NativeIO$Windows.access0 and
        // therefore needs hadoop.dll. Plain java.io does the same job here.
        File local = pathToFile(path);

        if (!local.exists()) {
            throw new FileNotFoundException("File " + path + " does not exist");
        }
        if (local.isFile()) {
            return new FileStatus[] { getFileStatus(path) };
        }

        File[] entries = local.listFiles();
        if (entries == null) {
            throw new IOException("Could not list directory: " + path);
        }

        FileStatus[] results = new FileStatus[entries.length];
        for (int i = 0; i < entries.length; i++) {
            results[i] = getFileStatus(new Path(path, entries[i].getName()));
        }
        return results;
    }
}
