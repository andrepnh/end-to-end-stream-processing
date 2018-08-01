package com.github.andrepnh.kafka.playground;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static com.google.common.base.Preconditions.checkState;

/**
 * Figures out host ports for every kafka node in the cluster
 */
public class PortInspector {
    public IntList inspect(int instances) {
        var ports = new IntArrayList();
        for (int i = 1; i <= instances; i++) {
            var container = "testbed_kafka_" + i;
            inspect(container);
        }
        return ports;
    }

    private int inspect(String container) {
        Process process;
        try {
            process = new ProcessBuilder()
                .command("docker inspect --format='{{(index (index .NetworkSettings.Ports \"9092/tcp\") 0).HostPort}}' "
                    + container)
                .start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        checkState(process.exitValue() == 0,
            "Docker inspect failed with exit code %s\n%s",
            process.exitValue(), readFully(process.getErrorStream()));
        String port = readFully(process.getInputStream());
        try {
            return Integer.parseInt(port);
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Docker inspect exited cleanly but did not return a valid port number: "
                + port, ex);
        }
    }

    private static String readFully(InputStream stream) {
        try {
            return CharStreams.toString(new InputStreamReader(stream, Charsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
