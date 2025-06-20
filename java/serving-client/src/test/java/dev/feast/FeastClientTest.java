/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.feast;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRangeRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRangeResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FeastClientTest {
  private final String AUTH_TOKEN = "test token";
  private final long TIMEOUT_MILLIS = 300;

  @Rule public GrpcCleanupRule grpcRule;
  private AtomicBoolean isAuthenticated;

  private ServingServiceImplBase servingMock =
      mock(
          ServingServiceImplBase.class,
          delegatesTo(
              new ServingServiceImplBase() {
                @Override
                public void getOnlineFeatures(
                    GetOnlineFeaturesRequest request,
                    StreamObserver<GetOnlineFeaturesResponse> responseObserver) {

                  GetOnlineFeaturesRequest expectedRef =
                      FeastClientTest.getFakeOnlineFeaturesRefRequest();
                  GetOnlineFeaturesRequest expectedService =
                      FeastClientTest.getFakeOnlineFeaturesServiceRequest();
                  GetOnlineFeaturesRequest expectedWithoutStatus =
                      FeastClientTest.getFakeOnlineFeaturesRefRequestWithoutStatus();

                  System.out.println("=== RECEIVED REQUEST ===");
                  System.out.println(request);
                  System.out.println("=== EXPECTED REF REQUEST ===");
                  System.out.println(expectedRef);
                  System.out.println("=== EXPECTED SERVICE REQUEST ===");
                  System.out.println(expectedService);
                  System.out.println("=== EXPECTED WITHOUT STATUS REQUEST ===");
                  System.out.println(expectedWithoutStatus);

                  System.out.println("=== COMPARISON DETAILS ===");
                  System.out.println(
                      "Request feature service: '" + request.getFeatureService() + "'");
                  System.out.println("Request has features: " + request.hasFeatures());
                  if (request.hasFeatures()) {
                    System.out.println("Request features: " + request.getFeatures().getValList());
                  }
                  System.out.println("Request entities: " + request.getEntitiesMap());
                  System.out.println("Request include metadata: " + request.getIncludeMetadata());

                  boolean matchesRef = request.equals(expectedRef);
                  boolean matchesService = request.equals(expectedService);
                  boolean matchesWithoutStatus = request.equals(expectedWithoutStatus);

                  System.out.println("Matches ref: " + matchesRef);
                  System.out.println("Matches service: " + matchesService);
                  System.out.println("Matches without status: " + matchesWithoutStatus);

                  if (!matchesRef && !matchesService && !matchesWithoutStatus) {
                    System.out.println("REJECTING REQUEST - NO MATCH FOUND");
                    responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
                    return;
                  }

                  responseObserver.onNext(FeastClientTest.getFakeOnlineFeaturesResponse());
                  responseObserver.onCompleted();
                }

                @Override
                public void getOnlineFeaturesRange(
                    GetOnlineFeaturesRangeRequest request,
                    StreamObserver<GetOnlineFeaturesRangeResponse> responseObserver) {

                  GetOnlineFeaturesRangeRequest expected =
                      FeastClientTest.getFakeOnlineFeaturesRangeRequest();

                  System.out.println("=== RECEIVED RANGE REQUEST ===");
                  System.out.println(request);
                  System.out.println("=== EXPECTED RANGE REQUEST ===");
                  System.out.println(expected);

                  System.out.println("=== RANGE COMPARISON DETAILS ===");
                  System.out.println("Request features: " + request.getFeatures().getValList());
                  System.out.println("Request entities: " + request.getEntitiesMap());
                  System.out.println(
                      "Request sort key filters count: " + request.getSortKeyFiltersCount());
                  System.out.println("Request limit: " + request.getLimit());
                  System.out.println(
                      "Request reverse sort order: " + request.getReverseSortOrder());
                  System.out.println("Request include metadata: " + request.getIncludeMetadata());

                  boolean matches = request.equals(expected);
                  System.out.println("Matches expected: " + matches);

                  if (!matches) {
                    System.out.println("REJECTING RANGE REQUEST - NO MATCH FOUND");
                    responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
                    return;
                  }

                  responseObserver.onNext(FeastClientTest.getFakeOnlineFeaturesRangeResponse());
                  responseObserver.onCompleted();
                }
              }));

  private FeastClient client;

  @Before
  public void setup() throws Exception {
    this.grpcRule = new GrpcCleanupRule();
    this.isAuthenticated = new AtomicBoolean(false);
    // setup fake serving service
    String serverName = InProcessServerBuilder.generateName();
    this.grpcRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(this.servingMock)
            .build()
            .start());

    // setup test feast client target
    ManagedChannel channel =
        this.grpcRule.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
    this.client = new FeastClient(channel, Optional.empty(), TIMEOUT_MILLIS);
  }

  @Test
  public void shouldGetOnlineFeatures() {
    shouldGetOnlineFeaturesFeatureRef(this.client);
  }

  @Test
  public void shouldGetOnlineFeaturesFeatureService() {
    shouldGetOnlineFeaturesFeatureService(this.client);
  }

  @Test
  public void shouldGetOnlineFeaturesWithoutStatus() {
    shouldGetOnlineFeaturesWithoutStatus(this.client);
  }

  @Test
  public void shouldGetOnlineFeaturesRange() {
    shouldGetOnlineFeaturesRangeWithClient(this.client);
  }

  private void shouldGetOnlineFeaturesFeatureRef(FeastClient client) {
    List<Row> rows =
        client.getOnlineFeatures(
            Arrays.asList("driver:name", "driver:rating", "driver:null_value"),
            Arrays.asList(
                Row.create().set("driver_id", 1).setEntityTimestamp(Instant.ofEpochSecond(100))),
            "driver_project");

    assertEquals(
        new HashMap<String, Value>() {
          {
            put("driver_id", intValue(1));
            put("driver:name", strValue("david"));
            put("driver:rating", intValue(3));
            put("driver:null_value", Value.newBuilder().build());
          }
        },
        rows.get(0).getFields());
    assertEquals(new HashMap<String, FieldStatus>() {}, rows.get(0).getStatuses());
  }

  private void shouldGetOnlineFeaturesFeatureService(FeastClient client) {
    // Only responbility of the client test is to make sure that the proto message is created
    // properly/rows are translated properly.
    List<Row> rows =
        client.getOnlineFeatures(
            "driver_service",
            Arrays.asList(
                Row.create().set("driver_id", 1).setEntityTimestamp(Instant.ofEpochSecond(100))),
            "driver_project");

    assertEquals(
        new HashMap<String, Value>() {
          {
            put("driver_id", intValue(1));
            put("driver:name", strValue("david"));
            put("driver:rating", intValue(3));
            put("driver:null_value", Value.newBuilder().build());
          }
        },
        rows.get(0).getFields());
    assertEquals(new HashMap<String, FieldStatus>() {}, rows.get(0).getStatuses());
  }

  private void shouldGetOnlineFeaturesWithoutStatus(FeastClient client) {

    List<Row> rows =
        client.getOnlineFeatures(
            getFakeOnlineFeaturesRefRequestWithoutStatus(),
            Collections.singletonList(Row.create().set("driver_id", 1)),
            "driver_project");

    assertEquals(
        new HashMap<String, Value>() {
          {
            put("driver_id", intValue(1));
            put("driver:name", strValue("david"));
            put("driver:rating", intValue(3));
            put("driver:null_value", Value.newBuilder().build());
          }
        },
        rows.get(0).getFields());

    for (String fieldName : rows.get(0).getFields().keySet()) {
      assertNull(
          "Field " + fieldName + " should not have a status", rows.get(0).getStatus(fieldName));
    }
  }

  private void shouldGetOnlineFeaturesRangeWithClient(FeastClient client) {
    List<RangeRow> rows =
        client.getOnlineFeaturesRange(
            Arrays.asList("driver:name", "driver:rating", "driver:null_value"),
            Arrays.asList(Row.create().set("driver_id", 1)),
            Arrays.asList(
                new SortKeyFilterModel("event_timestamp", LocalDateTime.of(2025, 5, 1, 0, 0)),
                new SortKeyFilterModel("sort_key", new RangeQueryModel(2.5f, 5.0f, true, false))),
            10,
            false,
            "driver_project");

    assertEquals(
        new HashMap<String, Value>() {
          {
            put("driver_id", intValue(1));
          }
        },
        rows.get(0).getEntity());
    assertEquals(
        new HashMap<String, List<Value>>() {
          {
            put("driver:name", Arrays.asList(strValue("david")));
            put("driver:rating", Arrays.asList(intValue(3)));
            put("driver:null_value", Arrays.asList(Value.newBuilder().build()));
          }
        },
        rows.get(0).getFields());
    assertEquals(new HashMap<String, List<FieldStatus>>() {}, rows.get(0).getStatuses());
  }

  private static GetOnlineFeaturesRequest getFakeOnlineFeaturesRefRequest() {
    // setup mock serving service stub
    return GetOnlineFeaturesRequest.newBuilder()
        .setFeatures(
            ServingAPIProto.FeatureList.newBuilder()
                .addVal("driver:name")
                .addVal("driver:rating")
                .addVal("driver:null_value")
                .build())
        .putEntities("driver_id", ValueProto.RepeatedValue.newBuilder().addVal(intValue(1)).build())
        .setIncludeMetadata(false)
        .build();
  }

  private static GetOnlineFeaturesRequest getFakeOnlineFeaturesRefRequestWithoutStatus() {
    // setup mock serving service stub
    return GetOnlineFeaturesRequest.newBuilder()
        .setFeatures(
            ServingAPIProto.FeatureList.newBuilder()
                .addVal("driver:name")
                .addVal("driver:rating")
                .addVal("driver:null_value")
                .build())
        .putEntities("driver_id", ValueProto.RepeatedValue.newBuilder().addVal(intValue(1)).build())
        .setIncludeMetadata(false)
        .build();
  }

  private static GetOnlineFeaturesRequest getFakeOnlineFeaturesServiceRequest() {
    // setup mock serving service stub
    return GetOnlineFeaturesRequest.newBuilder()
        .setFeatureService("driver_service")
        .putEntities("driver_id", ValueProto.RepeatedValue.newBuilder().addVal(intValue(1)).build())
        .setIncludeMetadata(false)
        .build();
  }

  private static GetOnlineFeaturesResponse getFakeOnlineFeaturesResponse() {
    return GetOnlineFeaturesResponse.newBuilder()
        .addResults(
            GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                .addValues(strValue("david"))
                .build())
        .addResults(
            GetOnlineFeaturesResponse.FeatureVector.newBuilder().addValues(intValue(3)).build())
        .addResults(
            GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                .addValues(Value.newBuilder().build())
                .build())
        .setMetadata(
            ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                .setFeatureNames(
                    ServingAPIProto.FeatureList.newBuilder()
                        .addVal("driver:name")
                        .addVal("driver:rating")
                        .addVal("driver:null_value"))
                .build())
        .build();
  }

  private static GetOnlineFeaturesRangeRequest getFakeOnlineFeaturesRangeRequest() {
    return GetOnlineFeaturesRangeRequest.newBuilder()
        .setFeatures(
            ServingAPIProto.FeatureList.newBuilder()
                .addVal("driver:name")
                .addVal("driver:rating")
                .addVal("driver:null_value")
                .build())
        .addAllSortKeyFilters(
            Arrays.asList(
                ServingAPIProto.SortKeyFilter.newBuilder()
                    .setSortKeyName("event_timestamp")
                    .setEquals(Value.newBuilder().setUnixTimestampVal(1746057600000L).build())
                    .build(),
                ServingAPIProto.SortKeyFilter.newBuilder()
                    .setSortKeyName("sort_key")
                    .setRange(
                        ServingAPIProto.SortKeyFilter.RangeQuery.newBuilder()
                            .setRangeStart(Value.newBuilder().setFloatVal(2.5f).build())
                            .setRangeEnd(Value.newBuilder().setFloatVal(5.0f).build())
                            .setStartInclusive(true)
                            .setEndInclusive(false))
                    .build()))
        .setLimit(10)
        .setReverseSortOrder(false)
        .setIncludeMetadata(false)
        .build();
  }

  private static GetOnlineFeaturesRangeResponse getFakeOnlineFeaturesRangeResponse() {
    return GetOnlineFeaturesRangeResponse.newBuilder()
        .addResults(
            GetOnlineFeaturesRangeResponse.RangeFeatureVector.newBuilder()
                .addValues(repeatedValue(strValue("david")))
                .build())
        .addResults(
            GetOnlineFeaturesRangeResponse.RangeFeatureVector.newBuilder()
                .addValues(repeatedValue(intValue(3)))
                .build())
        .addResults(
            GetOnlineFeaturesRangeResponse.RangeFeatureVector.newBuilder()
                .addValues(repeatedValue(Value.newBuilder().build()))
                .build())
        .setMetadata(
            ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                .setFeatureNames(
                    ServingAPIProto.FeatureList.newBuilder()
                        .addVal("driver:name")
                        .addVal("driver:rating")
                        .addVal("driver:null_value"))
                .build())
        .build();
  }

  private static Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }

  private static Value intValue(int val) {
    return Value.newBuilder().setInt32Val(val).build();
  }

  private static Value timestampValue(long val) {
    return Value.newBuilder().setUnixTimestampVal(val).build();
  }

  private static ValueProto.RepeatedValue repeatedValue(Value val) {
    return ValueProto.RepeatedValue.newBuilder().addVal(val).build();
  }

  private static ServingAPIProto.RepeatedFieldStatus repeatedStatus(FieldStatus val) {
    return ServingAPIProto.RepeatedFieldStatus.newBuilder().addStatus(val).build();
  }
}
