package com.pointcarbon.loaders.WBMS;

import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.loaderframework.service.DownloadHelper;
import com.pointcarbon.loaderframework.service.ILoader;
import com.pointcarbon.loaderframework.service.LoaderRunService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class FTPLoader implements ILoader {
    private static final Logger log = LoggerFactory.getLogger(FTPLoader.class);
    private static final String START_DATE_KEY = "startdate_offset";
    private static final String END_DATE_KEY = "enddate_offset";
    private static final int PROXY_INDEX = 0;
    private static final int USERNAME_INDEX = 0;
    private static final int PORT_INDEX = 1;
    private static final int PASSWORD_INDEX = 1;
    private static final int REGEX_INDEX = 2;
    private static final int ARRAY_SIZE_WITH_REGEX = 3;

    @Override
    public void load(DateTime focusDate, DownloadHelper downloadHelper, EsbMessage message) throws Exception {
        int apexStartDateOffset = Integer.parseInt(message.getParameterValue(START_DATE_KEY));
        int apexEndDateOffset = Integer.parseInt(message.getParameterValue(END_DATE_KEY));

        LocalDate apexStartDate = new LocalDate().plusDays(apexStartDateOffset);
        LocalDate apexEndDate = new LocalDate().plusDays(apexEndDateOffset);

        String[] proxyPortRegex = message.getParameterValue(LoaderRunService.PARAMETER).split(";");
        String[] usernamePassword = message.getParameterValue(LoaderRunService.PASSWORD).split(";");

        String path = message.getParameterValue(LoaderRunService.USERNAME);
        String proxy = proxyPortRegex[PROXY_INDEX].trim();
        String username = usernamePassword[USERNAME_INDEX].trim();
        String password = usernamePassword[PASSWORD_INDEX].trim();
        String regex = getRegex(proxyPortRegex);
        int port = getPort(proxyPortRegex);

        FTPClient ftp = open(proxy, port, username, password);
        showLogs(ftp, proxy, username);

        try {
            if (ftp.isConnected()) {
                ftp.changeWorkingDirectory(path);
                FTPFile[] allFiles = ftp.listFiles();
                for (FTPFile file : allFiles) {
                    if (file.getType() == FTPFile.FILE_TYPE) {
                        LocalDate ftpFileDateTime = getDateTime(file);
                        if (!ftpFileDateTime.isBefore(apexStartDate) && !ftpFileDateTime.isAfter(apexEndDate)) {
                            String pathToRemoteFile = createPathToRemoteFile(path, file);
                            sendFile(downloadHelper, ftp, file, pathToRemoteFile, regex);
                        }
                    }
                }
            }
        } catch (IOException exception) {
            log.error("First try. Error closing ftp client", exception);
        } finally {
            if (ftp.isConnected()) {
                close(ftp);
            }
        }
    }

    private void showLogs(FTPClient ftp, String host, String username) {
        log.info("replystring={}", StringUtils.trim(ftp.getReplyString()));
        log.info("replystring={}", StringUtils.trim(ftp.getReplyString()));
        log.info("Login to server - servername={}, username={}", host, username);
        log.info("replystring={}", StringUtils.trim(ftp.getReplyString()));
    }

    private FTPClient open(String proxy, int port, String username, String password) throws IOException {
        FTPClient client = new FTPClient();
        client.setConnectTimeout(60000);
        client.setDefaultTimeout(60000);
        client.setDataTimeout(600000);
        log.info("Connecting to proxy. proxyServer={}, proxyPort={}", proxy, port);
        client.connect(proxy, port);

        if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
            client.disconnect();
            throw new IOException("Exception in connecting to FTP Server :" + client.getReplyCode());
        }

        if (client.login(username, password)) {
            log.info("Login successful - user={}", username);
            log.info("replystring={}", StringUtils.trim(client.getReplyString()));
        } else {
            if (client.isConnected()) {
                close(client);
            }
            throw new RuntimeException("Login failed - replystring=" + StringUtils.trim(client.getReplyString()));
        }

        client.enterLocalPassiveMode();
        client.setFileType(FTP.BINARY_FILE_TYPE);
        return client;
    }

    private void close(FTPClient client) {
        try {
            client.logout();
            client.disconnect();
        } catch (IOException exception) {
            log.error("Error closing ftp client", exception);
        }
    }

    private void sendFile(DownloadHelper downloadHelper, FTPClient ftpClient, FTPFile file, String pathToRemoteFile, String regex) throws Exception {
        InputStream inputStream = ftpClient.retrieveFileStream(pathToRemoteFile);
        int statusCode = ftpClient.getReplyCode();
        if (inputStream != null && (FTPReply.isPositivePreliminary(statusCode) || FTPReply.isPositiveCompletion(statusCode))) {
            byte[] fileContent = IOUtils.toByteArray(inputStream);
            boolean isPositive = ftpClient.completePendingCommand();
            if (isPositive) {
                sendFileHelper(downloadHelper, fileContent, file.getName(), regex);
            }
            inputStream.close();
        } else {
            log.error("Get content failed, statusCode={}", statusCode);
        }
    }

    private void sendFileHelper(DownloadHelper downloadHelper, byte[] content, String fileName, String regex) {
        try {
            if (regex.isEmpty()) {
                sendFile(downloadHelper, content, fileName);
                log.info("File " + fileName + " has been downloaded successfully.");
            } else {
                if (fileName.matches(regex)) {
                    sendFile(downloadHelper, content, fileName);
                    log.info("File " + fileName + " has been downloaded successfully.");
                }
            }
        } catch (Exception exception) {
            log.info("File " + fileName + " hasn't been downloaded successfully.");
        }
    }

    private void sendFile(DownloadHelper downloadHelper, byte[] content, String fileName) throws Exception {
        downloadHelper.withModifiedContent(fileName, content).send();
    }

    private String getRegex(String[] proxyAndPortAndRegex) {
        if (proxyAndPortAndRegex.length == ARRAY_SIZE_WITH_REGEX) {
            return proxyAndPortAndRegex[REGEX_INDEX].trim();
        }
        return "";
    }

    private String createPathToRemoteFile(String path, FTPFile file) {
        return path + file.getName();
    }

    private LocalDate getDateTime(FTPFile file) {
        return new LocalDate(file.getTimestamp().getTimeInMillis());
    }

    private int getPort(String[] proxyPortRegex) {
        return Integer.parseInt(proxyPortRegex[PORT_INDEX].trim());
    }

}