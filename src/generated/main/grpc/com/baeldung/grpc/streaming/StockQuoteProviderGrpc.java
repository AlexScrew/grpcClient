package com.baeldung.grpc.streaming;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.60.1)",
    comments = "Source: stock_quote.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class StockQuoteProviderGrpc {

  private StockQuoteProviderGrpc() {}

  public static final java.lang.String SERVICE_NAME = "stockquote.StockQuoteProvider";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock,
      com.baeldung.grpc.streaming.StockQuote> getServerSideStreamingGetListStockQuotesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "serverSideStreamingGetListStockQuotes",
      requestType = com.baeldung.grpc.streaming.Stock.class,
      responseType = com.baeldung.grpc.streaming.StockQuote.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock,
      com.baeldung.grpc.streaming.StockQuote> getServerSideStreamingGetListStockQuotesMethod() {
    io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock, com.baeldung.grpc.streaming.StockQuote> getServerSideStreamingGetListStockQuotesMethod;
    if ((getServerSideStreamingGetListStockQuotesMethod = StockQuoteProviderGrpc.getServerSideStreamingGetListStockQuotesMethod) == null) {
      synchronized (StockQuoteProviderGrpc.class) {
        if ((getServerSideStreamingGetListStockQuotesMethod = StockQuoteProviderGrpc.getServerSideStreamingGetListStockQuotesMethod) == null) {
          StockQuoteProviderGrpc.getServerSideStreamingGetListStockQuotesMethod = getServerSideStreamingGetListStockQuotesMethod =
              io.grpc.MethodDescriptor.<com.baeldung.grpc.streaming.Stock, com.baeldung.grpc.streaming.StockQuote>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "serverSideStreamingGetListStockQuotes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.baeldung.grpc.streaming.Stock.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.baeldung.grpc.streaming.StockQuote.getDefaultInstance()))
              .setSchemaDescriptor(new StockQuoteProviderMethodDescriptorSupplier("serverSideStreamingGetListStockQuotes"))
              .build();
        }
      }
    }
    return getServerSideStreamingGetListStockQuotesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock,
      com.baeldung.grpc.streaming.StockQuote> getClientSideStreamingGetStatisticsOfStocksMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "clientSideStreamingGetStatisticsOfStocks",
      requestType = com.baeldung.grpc.streaming.Stock.class,
      responseType = com.baeldung.grpc.streaming.StockQuote.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock,
      com.baeldung.grpc.streaming.StockQuote> getClientSideStreamingGetStatisticsOfStocksMethod() {
    io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock, com.baeldung.grpc.streaming.StockQuote> getClientSideStreamingGetStatisticsOfStocksMethod;
    if ((getClientSideStreamingGetStatisticsOfStocksMethod = StockQuoteProviderGrpc.getClientSideStreamingGetStatisticsOfStocksMethod) == null) {
      synchronized (StockQuoteProviderGrpc.class) {
        if ((getClientSideStreamingGetStatisticsOfStocksMethod = StockQuoteProviderGrpc.getClientSideStreamingGetStatisticsOfStocksMethod) == null) {
          StockQuoteProviderGrpc.getClientSideStreamingGetStatisticsOfStocksMethod = getClientSideStreamingGetStatisticsOfStocksMethod =
              io.grpc.MethodDescriptor.<com.baeldung.grpc.streaming.Stock, com.baeldung.grpc.streaming.StockQuote>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "clientSideStreamingGetStatisticsOfStocks"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.baeldung.grpc.streaming.Stock.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.baeldung.grpc.streaming.StockQuote.getDefaultInstance()))
              .setSchemaDescriptor(new StockQuoteProviderMethodDescriptorSupplier("clientSideStreamingGetStatisticsOfStocks"))
              .build();
        }
      }
    }
    return getClientSideStreamingGetStatisticsOfStocksMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock,
      com.baeldung.grpc.streaming.StockQuote> getBidirectionalStreamingGetListsStockQuotesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "bidirectionalStreamingGetListsStockQuotes",
      requestType = com.baeldung.grpc.streaming.Stock.class,
      responseType = com.baeldung.grpc.streaming.StockQuote.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock,
      com.baeldung.grpc.streaming.StockQuote> getBidirectionalStreamingGetListsStockQuotesMethod() {
    io.grpc.MethodDescriptor<com.baeldung.grpc.streaming.Stock, com.baeldung.grpc.streaming.StockQuote> getBidirectionalStreamingGetListsStockQuotesMethod;
    if ((getBidirectionalStreamingGetListsStockQuotesMethod = StockQuoteProviderGrpc.getBidirectionalStreamingGetListsStockQuotesMethod) == null) {
      synchronized (StockQuoteProviderGrpc.class) {
        if ((getBidirectionalStreamingGetListsStockQuotesMethod = StockQuoteProviderGrpc.getBidirectionalStreamingGetListsStockQuotesMethod) == null) {
          StockQuoteProviderGrpc.getBidirectionalStreamingGetListsStockQuotesMethod = getBidirectionalStreamingGetListsStockQuotesMethod =
              io.grpc.MethodDescriptor.<com.baeldung.grpc.streaming.Stock, com.baeldung.grpc.streaming.StockQuote>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "bidirectionalStreamingGetListsStockQuotes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.baeldung.grpc.streaming.Stock.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.baeldung.grpc.streaming.StockQuote.getDefaultInstance()))
              .setSchemaDescriptor(new StockQuoteProviderMethodDescriptorSupplier("bidirectionalStreamingGetListsStockQuotes"))
              .build();
        }
      }
    }
    return getBidirectionalStreamingGetListsStockQuotesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static StockQuoteProviderStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StockQuoteProviderStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StockQuoteProviderStub>() {
        @java.lang.Override
        public StockQuoteProviderStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StockQuoteProviderStub(channel, callOptions);
        }
      };
    return StockQuoteProviderStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static StockQuoteProviderBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StockQuoteProviderBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StockQuoteProviderBlockingStub>() {
        @java.lang.Override
        public StockQuoteProviderBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StockQuoteProviderBlockingStub(channel, callOptions);
        }
      };
    return StockQuoteProviderBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static StockQuoteProviderFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StockQuoteProviderFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StockQuoteProviderFutureStub>() {
        @java.lang.Override
        public StockQuoteProviderFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StockQuoteProviderFutureStub(channel, callOptions);
        }
      };
    return StockQuoteProviderFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void serverSideStreamingGetListStockQuotes(com.baeldung.grpc.streaming.Stock request,
        io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getServerSideStreamingGetListStockQuotesMethod(), responseObserver);
    }

    /**
     */
    default io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.Stock> clientSideStreamingGetStatisticsOfStocks(
        io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getClientSideStreamingGetStatisticsOfStocksMethod(), responseObserver);
    }

    /**
     */
    default io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.Stock> bidirectionalStreamingGetListsStockQuotes(
        io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getBidirectionalStreamingGetListsStockQuotesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service StockQuoteProvider.
   */
  public static abstract class StockQuoteProviderImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return StockQuoteProviderGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service StockQuoteProvider.
   */
  public static final class StockQuoteProviderStub
      extends io.grpc.stub.AbstractAsyncStub<StockQuoteProviderStub> {
    private StockQuoteProviderStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StockQuoteProviderStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StockQuoteProviderStub(channel, callOptions);
    }

    /**
     */
    public void serverSideStreamingGetListStockQuotes(com.baeldung.grpc.streaming.Stock request,
        io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getServerSideStreamingGetListStockQuotesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.Stock> clientSideStreamingGetStatisticsOfStocks(
        io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
          getChannel().newCall(getClientSideStreamingGetStatisticsOfStocksMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.Stock> bidirectionalStreamingGetListsStockQuotes(
        io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getBidirectionalStreamingGetListsStockQuotesMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service StockQuoteProvider.
   */
  public static final class StockQuoteProviderBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<StockQuoteProviderBlockingStub> {
    private StockQuoteProviderBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StockQuoteProviderBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StockQuoteProviderBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<com.baeldung.grpc.streaming.StockQuote> serverSideStreamingGetListStockQuotes(
        com.baeldung.grpc.streaming.Stock request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getServerSideStreamingGetListStockQuotesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service StockQuoteProvider.
   */
  public static final class StockQuoteProviderFutureStub
      extends io.grpc.stub.AbstractFutureStub<StockQuoteProviderFutureStub> {
    private StockQuoteProviderFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StockQuoteProviderFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StockQuoteProviderFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SERVER_SIDE_STREAMING_GET_LIST_STOCK_QUOTES = 0;
  private static final int METHODID_CLIENT_SIDE_STREAMING_GET_STATISTICS_OF_STOCKS = 1;
  private static final int METHODID_BIDIRECTIONAL_STREAMING_GET_LISTS_STOCK_QUOTES = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SERVER_SIDE_STREAMING_GET_LIST_STOCK_QUOTES:
          serviceImpl.serverSideStreamingGetListStockQuotes((com.baeldung.grpc.streaming.Stock) request,
              (io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CLIENT_SIDE_STREAMING_GET_STATISTICS_OF_STOCKS:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.clientSideStreamingGetStatisticsOfStocks(
              (io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote>) responseObserver);
        case METHODID_BIDIRECTIONAL_STREAMING_GET_LISTS_STOCK_QUOTES:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.bidirectionalStreamingGetListsStockQuotes(
              (io.grpc.stub.StreamObserver<com.baeldung.grpc.streaming.StockQuote>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getServerSideStreamingGetListStockQuotesMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.baeldung.grpc.streaming.Stock,
              com.baeldung.grpc.streaming.StockQuote>(
                service, METHODID_SERVER_SIDE_STREAMING_GET_LIST_STOCK_QUOTES)))
        .addMethod(
          getClientSideStreamingGetStatisticsOfStocksMethod(),
          io.grpc.stub.ServerCalls.asyncClientStreamingCall(
            new MethodHandlers<
              com.baeldung.grpc.streaming.Stock,
              com.baeldung.grpc.streaming.StockQuote>(
                service, METHODID_CLIENT_SIDE_STREAMING_GET_STATISTICS_OF_STOCKS)))
        .addMethod(
          getBidirectionalStreamingGetListsStockQuotesMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.baeldung.grpc.streaming.Stock,
              com.baeldung.grpc.streaming.StockQuote>(
                service, METHODID_BIDIRECTIONAL_STREAMING_GET_LISTS_STOCK_QUOTES)))
        .build();
  }

  private static abstract class StockQuoteProviderBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    StockQuoteProviderBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.baeldung.grpc.streaming.StockQuoteProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("StockQuoteProvider");
    }
  }

  private static final class StockQuoteProviderFileDescriptorSupplier
      extends StockQuoteProviderBaseDescriptorSupplier {
    StockQuoteProviderFileDescriptorSupplier() {}
  }

  private static final class StockQuoteProviderMethodDescriptorSupplier
      extends StockQuoteProviderBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    StockQuoteProviderMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (StockQuoteProviderGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new StockQuoteProviderFileDescriptorSupplier())
              .addMethod(getServerSideStreamingGetListStockQuotesMethod())
              .addMethod(getClientSideStreamingGetStatisticsOfStocksMethod())
              .addMethod(getBidirectionalStreamingGetListsStockQuotesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
