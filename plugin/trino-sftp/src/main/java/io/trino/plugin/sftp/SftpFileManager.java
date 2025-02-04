/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

public class SftpFileManager
        implements AutoCloseable
{
    private final JSch jsch;
    private Session session;
    private ChannelSftp channel;
    private final SftpConfig config;
    private final int maxRetries;

    public SftpFileManager(SftpConfig config)
    {
        this.config = config;
        this.jsch = new JSch();
        this.maxRetries = config.getMaxRetries();
    }

    private void connect()
            throws IOException
    {
        try {
            if (session == null || !session.isConnected()) {
                session = jsch.getSession(config.getUsername(), config.getHost(), config.getPort());
                session.setPassword(config.getPassword());
                session.setConfig("StrictHostKeyChecking", "no");
                session.connect(config.getConnectionTimeout());

                channel = (ChannelSftp) session.openChannel("sftp");
                channel.connect();
            }
        }
        catch (JSchException e) {
            throw new IOException("Failed to connect to SFTP", e);
        }
    }

    public List<SftpFile> listFiles(String directory, String compressionType)
            throws IOException
    {
        List<SftpFile> files = new ArrayList<>();
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                connect();
                Vector<ChannelSftp.LsEntry> entries = channel.ls(directory);

                for (ChannelSftp.LsEntry entry : entries) {
                    if (!entry.getAttrs().isDir() && isCsvFile(entry.getFilename(), compressionType)) {
                        files.add(new SftpFile(
                                directory + "/" + entry.getFilename(),
                                entry.getAttrs().getSize(),
                                entry.getAttrs().getMTime()));
                    }
                }
                break;
            }
            catch (SftpException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    throw new RuntimeException("Failed to list files after " + maxRetries + " retries", e);
                }
                reconnect();
            }
        }
        return files;
    }

    public InputStream getInputStream(String path, String compressionType)
            throws IOException
    {
        try {
            connect();
            InputStream is = channel.get(path);

            return switch (compressionType.toLowerCase(Locale.ENGLISH)) {
                case "gzip" -> new GZIPInputStream(is);
//                case "snappy" -> new SnappyInputStream(is);
                case "none" -> is;
                default -> throw new IllegalArgumentException("Unsupported compression type: " + compressionType);
            };
        }
        catch (SftpException e) {
            throw new IOException("Failed to open file: " + path, e);
        }
    }

    private void reconnect()
    {
        try {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
            connect();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to reconnect to SFTP server", e);
        }
    }

    private boolean isCsvFile(String filename, String compressionType)
    {
        String lowerFilename = filename.toLowerCase(Locale.ENGLISH);
        return switch (compressionType.toLowerCase(Locale.ENGLISH)) {
            case "gzip" -> lowerFilename.endsWith(".csv.gz");
            case "snappy" -> lowerFilename.endsWith(".csv.snappy");
            case "zip" -> lowerFilename.endsWith(".csv.zip");
            default -> lowerFilename.endsWith(".csv");
        };
    }

    @Override
    public void close()
    {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }
}
