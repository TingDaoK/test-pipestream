package com.test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.crt.Log;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class TestPipeStream {
    private static final Logger logger = LoggerFactory.getLogger(TestPipeStream.class);

    public static void main(String... args) throws Exception {
        Log.initLoggingToFile(Log.LogLevel.Trace, "newlog.txt");
        S3AsyncClient client = S3AsyncClient.crtBuilder().build();
        S3TransferManager mgr = S3TransferManager.builder().s3Client(client).build();
        Path file = Path.of("/Users/dengket/project/graalvm/test-crt/sample-project-crt/500MB.txt");
        long length = file.toFile().length();
        ArrayList<PipedOutputStream> streams = new ArrayList<PipedOutputStream>();
        ArrayList<InputStream> input_streams = new ArrayList<InputStream>();
        var futures = new ArrayList<CompletableFuture<CompletedUpload>>();
        ExecutorService uploadExecutor = Executors.newFixedThreadPool(50);  // Define thread pool for upload tasks
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) uploadExecutor;
        for (int i =0; i < 5; i++) {
            var putRequest = PutObjectRequest.builder().bucket("aws-c-s3-test-bucket-099565").key("prefix/" + i).build();
            PipedOutputStream outputStream = new PipedOutputStream();
            streams.add(outputStream);
            InputStream inputStream = new PipedInputStream(outputStream);
            input_streams.add(inputStream);

            var uploadRequest = UploadRequest.builder()
                    .putObjectRequest(putRequest)
                    .requestBody(AsyncRequestBody.fromInputStream(inputStream, length, uploadExecutor))
                    .build();

            futures.add(mgr.upload(uploadRequest).completionFuture());
        }
//
        int activeCount = threadPool.getActiveCount();
        System.out.println(activeCount);
        int chunkSize = 8*1024*1024; // 8 MB in bytes
        ExecutorService writeExecutor = Executors.newFixedThreadPool(2);  // Define thread pool for upload tasks
        ThreadPoolExecutor threadPool2 = (ThreadPoolExecutor) uploadExecutor;
        activeCount = threadPool2.getActiveCount();
        System.out.println(activeCount);
        try (var fis = new BufferedReader(new FileReader(file.toFile()))) {
            char[] buffer = new char[chunkSize];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                    String dataChunk = new String(buffer, 0, bytesRead);
                    logger.info("Read !!!!");
                    for (int j = 0; j < streams.size(); j++) {
                        int finalJ = j;
                        writeExecutor.submit(() -> {
                            try {
                                logger.info("before writing to stream=" + finalJ + "  bytes="+ dataChunk.length());
                                streams.get(finalJ).write((dataChunk + "\n").getBytes());
                                logger.info("after writing to stream=" + finalJ + "  bytes="+ dataChunk.length());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

            }
        }
        activeCount = threadPool2.getActiveCount();
        System.out.println(activeCount);

        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        // Close all PipedOutputStreams after writing is done
        for (var stream : streams) {
            stream.close();
        }
        mgr.close();
        client.close();
        uploadExecutor.shutdown();
    }
}
