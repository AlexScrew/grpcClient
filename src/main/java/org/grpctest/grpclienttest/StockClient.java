package org.grpctest.grpclienttest;

import java.util.List;
import java.util.Map;

public interface StockClient {
    Map<String, List<String>> run() throws InterruptedException;
}
