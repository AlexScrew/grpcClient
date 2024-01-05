package org.grpctest.grpclienttest;

import com.baeldung.grpc.streaming.Stock;
import com.baeldung.grpc.streaming.StockQuote;
import com.baeldung.grpc.streaming.StockQuoteProviderGrpc;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class StockClientImpl implements StockClient {
    private List<Stock> stocks;
    @Value("${server.target:127.0.0.1:8980}")
    String target = "127.0.0.1:8980";

    @Override
    public Map<String, List<String>> run() throws InterruptedException {
        Map<String, List<String>> returnData = new HashMap<>();
        initializeStocks();
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        try {
            Client client = new Client(channel);
            returnData.put("serverSideStreamingListOfStockPrices", client.serverSideStreamingListOfStockPrices());
            returnData.put("clientSideStreamingGetStatisticsOfStocks", client.clientSideStreamingGetStatisticsOfStocks());
            returnData.put("bidirectionalStreamingGetListsStockQuotes", client.bidirectionalStreamingGetListsStockQuotes());
        } finally {
            channel.shutdownNow()
                    .awaitTermination(5, TimeUnit.SECONDS);
        }
        return returnData;
    }

    private void initializeStocks() {
        this.stocks = Arrays.asList(Stock.newBuilder().setTickerSymbol("AU").setCompanyName("Auburn Corp").setDescription("Aptitude Intel").build()
                , Stock.newBuilder().setTickerSymbol("BAS").setCompanyName("Bassel Corp").setDescription("Business Intel").build()
                , Stock.newBuilder().setTickerSymbol("COR").setCompanyName("Corvine Corp").setDescription("Corporate Intel").build()
                , Stock.newBuilder().setTickerSymbol("DIA").setCompanyName("Dialogic Corp").setDescription("Development Intel").build()
                , Stock.newBuilder().setTickerSymbol("EUS").setCompanyName("Euskaltel Corp").setDescription("English Intel").build());
    }

    public class Client {
        private final StockQuoteProviderGrpc.StockQuoteProviderBlockingStub blockingStub;
        private final StockQuoteProviderGrpc.StockQuoteProviderStub nonBlockingStub;

        public Client(Channel channel) {

            blockingStub = StockQuoteProviderGrpc.newBlockingStub(channel);
            nonBlockingStub = StockQuoteProviderGrpc.newStub(channel);
        }

        public List<String> serverSideStreamingListOfStockPrices() {
            List<String> returnData = new ArrayList<>();
            log.info("######START EXAMPLE######: ServerSideStreaming - list of Stock prices from a given stock");
            Stock request = Stock.newBuilder()
                    .setTickerSymbol("AU")
                    .setCompanyName("Austich")
                    .setDescription("server streaming example")
                    .build();
            Iterator<StockQuote> stockQuotes;
            try {
                log.info("REQUEST - ticker symbol {}", request.getTickerSymbol());
                stockQuotes = blockingStub.serverSideStreamingGetListStockQuotes(request);
                for (int i = 1; stockQuotes.hasNext(); i++) {
                    StockQuote stockQuote = stockQuotes.next();
                    returnData.add("RESPONSE - Price #" + i + ": {}" + stockQuote.getPrice());
                    log.info("RESPONSE - Price #" + i + ": {}", stockQuote.getPrice());
                }
            } catch (StatusRuntimeException e) {
                log.info("RPC failed: {}", e.getStatus());
            }
            return returnData;
        }

        public List<String> clientSideStreamingGetStatisticsOfStocks() throws InterruptedException {
            List<String> returnData = new ArrayList<>();
            log.info("######START EXAMPLE######: ClientSideStreaming - getStatisticsOfStocks from a list of stocks");
            final CountDownLatch finishLatch = new CountDownLatch(1);
            StreamObserver<StockQuote> responseObserver = new StreamObserver<StockQuote>() {
                @Override
                public void onNext(StockQuote summary) {
                    returnData.add("RESPONSE, got stock statistics - Average Price: {}, description: {}" + summary.getPrice() + summary.getDescription());
                    log.info("RESPONSE, got stock statistics - Average Price: {}, description: {}", summary.getPrice(), summary.getDescription());
                }

                @Override
                public void onCompleted() {
                    log.info("Finished clientSideStreamingGetStatisticsOfStocks");
                    finishLatch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    log.warn("Stock Statistics Failed: {}", Status.fromThrowable(t));
                    finishLatch.countDown();
                }

            };

            StreamObserver<Stock> requestObserver = nonBlockingStub.clientSideStreamingGetStatisticsOfStocks(responseObserver);
            try {

                for (Stock stock : stocks) {
                    log.info("REQUEST: {}, {}", stock.getTickerSymbol(), stock.getCompanyName());
                    requestObserver.onNext(stock);
                    if (finishLatch.getCount() == 0) {
                        return returnData;
                    }
                }
            } catch (RuntimeException e) {
                requestObserver.onError(e);
                throw e;
            }
            requestObserver.onCompleted();
            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                log.warn("clientSideStreamingGetStatisticsOfStocks can not finish within 1 minutes");
            }
            return null;
        }

        public List<String> bidirectionalStreamingGetListsStockQuotes() throws InterruptedException {
            List<String> returnData = Collections.EMPTY_LIST;
            log.info("#######START EXAMPLE#######: BidirectionalStreaming - getListsStockQuotes from list of stocks");
            final CountDownLatch finishLatch = new CountDownLatch(1);
            StreamObserver<StockQuote> responseObserver = new StreamObserver<StockQuote>() {
                @Override
                public void onNext(StockQuote stockQuote) {
                    returnData.add("RESPONSE price#{} : {}, description:{}" + stockQuote.getOfferNumber() + stockQuote.getPrice() + stockQuote.getDescription());
                    log.info("RESPONSE price#{} : {}, description:{}", stockQuote.getOfferNumber(), stockQuote.getPrice(), stockQuote.getDescription());
                }

                @Override
                public void onCompleted() {
                    log.info("Finished bidirectionalStreamingGetListsStockQuotes");
                    finishLatch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    log.warn("bidirectionalStreamingGetListsStockQuotes Failed: {0}", Status.fromThrowable(t));
                    finishLatch.countDown();
                }
            };
            StreamObserver<Stock> requestObserver = nonBlockingStub.bidirectionalStreamingGetListsStockQuotes(responseObserver);
            try {
                for (Stock stock : stocks) {
                    log.info("REQUEST: {}, {}", stock.getTickerSymbol(), stock.getCompanyName());
                    requestObserver.onNext(stock);
                    Thread.sleep(200);
                    if (finishLatch.getCount() == 0) {
                        return returnData;
                    }
                }
            } catch (RuntimeException e) {
                requestObserver.onError(e);
                throw e;
            }
            requestObserver.onCompleted();

            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                log.warn("bidirectionalStreamingGetListsStockQuotes can not finish within 1 minute");
            }
            return null;
        }
    }
}
