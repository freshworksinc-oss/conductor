/*
 * Copyright 2026 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.pgmirror;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** Just enough Redis (RESP2) for the mirror: RPUSH and LLEN, and OK for everything else. */
final class FakeRedisServer implements AutoCloseable {

    final Map<String, List<String>> lists = new ConcurrentHashMap<>();
    final List<List<String>> commands = new CopyOnWriteArrayList<>();
    private final ServerSocket server;

    FakeRedisServer() throws IOException {
        server = new ServerSocket(0);
        Thread t = new Thread(this::accept, "fake-redis");
        t.setDaemon(true);
        t.start();
    }

    int port() {
        return server.getLocalPort();
    }

    List<String> list(String key) {
        return lists.getOrDefault(key, List.of());
    }

    private void accept() {
        try {
            while (!server.isClosed()) {
                Socket s = server.accept();
                Thread h = new Thread(() -> serve(s), "fake-redis-conn");
                h.setDaemon(true);
                h.start();
            }
        } catch (IOException ignored) {
            // closed
        }
    }

    private void serve(Socket s) {
        try (s;
                InputStream in = new BufferedInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream()) {
            while (true) {
                List<String> cmd = readCommand(in);
                if (cmd == null) {
                    return;
                }
                commands.add(cmd);
                out.write(reply(cmd).getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException ignored) {
            // client went away
        }
    }

    private String reply(List<String> cmd) {
        switch (cmd.get(0).toUpperCase()) {
            case "RPUSH":
                List<String> l =
                        lists.computeIfAbsent(cmd.get(1), k -> new CopyOnWriteArrayList<>());
                l.addAll(cmd.subList(2, cmd.size()));
                return ":" + l.size() + "\r\n";
            case "LLEN":
                return ":" + list(cmd.get(1)).size() + "\r\n";
            case "PING":
                return "+PONG\r\n";
            default:
                return "+OK\r\n";
        }
    }

    private static List<String> readCommand(InputStream in) throws IOException {
        String head = line(in);
        if (head == null) {
            return null;
        }
        int n = Integer.parseInt(head.substring(1));
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int len = Integer.parseInt(line(in).substring(1));
            byte[] b = in.readNBytes(len);
            in.readNBytes(2); // CRLF
            parts.add(new String(b, StandardCharsets.UTF_8));
        }
        return parts;
    }

    private static String line(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                in.read(); // \n
                return buf.toString(StandardCharsets.UTF_8);
            }
            buf.write(c);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
}
