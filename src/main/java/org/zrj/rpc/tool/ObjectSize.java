package org.zrj.rpc.tool;

import org.zrj.raft.LogEntry;
import org.zrj.raft.message.AppendEntriesRequest;

// 仅测试需要
public class ObjectSize {
    public static long bytes(Object obj) {
        if (obj == null) {
            return 0;
        } else if (obj instanceof AppendEntriesRequest request) {
            int size = 0;
            for (LogEntry logEntry : request.getEntries()) {
                size += logEntry.getCommand().length();
            }
            return size;
        } else if (obj.getClass().isArray()) {
            Object[] objects = (Object[]) obj;
            if (objects.length == 0) {
                return 0;
            }

            int size = 0;
            if (objects[0] instanceof AppendEntriesRequest) {
                for (Object arg : objects) {
                    AppendEntriesRequest request = (AppendEntriesRequest) arg;
                    for (LogEntry logEntry : request.getEntries()) {
                        size += logEntry.getCommand().length();
                    }
                }
            }
            return size;
        } else {
            return 0;
        }
    }
}
