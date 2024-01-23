package com.shpp;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
public class Repo2 {



        // ... (existing code)

//        public void insertData(CqlSession session) {
//            Random random = new Random();
//            String[] storeAddress = generateStoreData(totalStores);
//            String[] categories = generateCategoryData(totalCategories);
//
//            category = categories[10];
//
//            // Create a fixed-size thread pool with 100 threads
//            ExecutorService executorService = Executors.newFixedThreadPool(100);
//
//            try {
//                // Create a CompletableFuture for each data insertion task
//                CompletableFuture<Void>[] futures = IntStream.range(0, totalStores)
//                        .parallel() // Enable parallel processing
//                        .mapToObj(i -> CompletableFuture.runAsync(() -> insertStoreData(session, categories, storeAddress, random), executorService))
//                        .toArray(CompletableFuture[]::new);
//
//                // Wait for all CompletableFuture tasks to complete
//                CompletableFuture.allOf(futures).join();
//            } finally {
//                // Shutdown the executor service
//                executorService.shutdown();
//            }
//
//            LOGGER.info("Data inserted successfully");
//        }
//
//        private void insertStoreData(CqlSession session, String[] categories, String[] storeAddress, Random random) {
//            for (int j = 1; j <= totalProducts; j++) {
//                Product product = new Product(UUID.randomUUID());
//                int quantity = random.nextInt(totalCategories);
//
//                executeInsert(session, categories[quantity], storeAddress[i - 1], product.getProductId(), quantity);
//            }
//        }

        // ... (existing code)
    }



