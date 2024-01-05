package org.grpctest.grpclienttest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class TestGrpcController {

    @Autowired
    StockClient stockClient;

    @GetMapping("/run")
    public Map<String, List<String>> run() throws InterruptedException {
        return stockClient.run();
    }
}
