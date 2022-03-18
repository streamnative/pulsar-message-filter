/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streamnative.pulsar.tag;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarCommandSenderImpl;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter.FilterResult;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test DefaultEntryFilter.
 */
public class TestDefaultEntryFilter {

    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ConfigurationCacheService configCacheService;
    private ChannelHandlerContext channelCtx;
    private LinkedBlockingQueue<CommandActiveConsumerChange> consumerChanges;
    private ZooKeeper mockZk;
    private PulsarService pulsar;
    final String successTopicName = "persistent://part-perf/sdsds/ptopic";

    /**
     * Before method setup for test.
     * @throws Exception
     */
    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();
        doReturn(createMockBookKeeper(mockZk, ForkJoinPool.commonPool()))
                .when(pulsar).getBookKeeperClient();

        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        doReturn(cache).when(pulsar).getLocalZkCache();

        configCacheService = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        LocalZooKeeperCacheService zkCache = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(any());
        doReturn(zkDataCache).when(zkCache).policiesCache();
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkCache).when(pulsar).getLocalZkCacheService();

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();

        consumerChanges = new LinkedBlockingQueue<>();
        this.channelCtx = mock(ChannelHandlerContext.class);
        doAnswer(invocationOnMock -> {
            ByteBuf buf = invocationOnMock.getArgument(0);

            ByteBuf cmdBuf = buf.retainedSlice(4, buf.writerIndex() - 4);
            try {
                int cmdSize = (int) cmdBuf.readUnsignedInt();
                int writerIndex = cmdBuf.writerIndex();
                cmdBuf.writerIndex(cmdBuf.readerIndex() + cmdSize);
                ByteBufCodedInputStream cmdInputStream = ByteBufCodedInputStream.get(cmdBuf);

                BaseCommand.Builder cmdBuilder = BaseCommand.newBuilder();
                BaseCommand cmd = cmdBuilder.mergeFrom(cmdInputStream, null).build();
                if (cmd.hasActiveConsumerChange()) {
                    consumerChanges.put(cmd.getActiveConsumerChange());
                }
                cmdBuilder.recycle();
                cmdBuf.writerIndex(writerIndex);
                cmdInputStream.recycle();

                cmd.recycle();
            } finally {
                cmdBuf.release();
            }

            return null;
        }).when(channelCtx).writeAndFlush(any(), any());

        serverCnx = spy(new ServerCnx(pulsar));
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();
        when(serverCnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12.getNumber());
        when(serverCnx.ctx()).thenReturn(channelCtx);
        doReturn(new PulsarCommandSenderImpl(null, serverCnx))
                .when(serverCnx).getCommandSender();

        NamespaceService nsSvc = mock(NamespaceService.class);
        doReturn(nsSvc).when(pulsar).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(nsSvc).isServiceUnitActive(any(TopicName.class));
        doReturn(CompletableFuture.completedFuture(true)).when(nsSvc).checkTopicOwnership(any(TopicName.class));

        setupMLAsyncCallbackMocks();

    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        if (brokerService != null) {
            brokerService.close();
            brokerService = null;
        }
        if (pulsar != null) {
            pulsar.close();
            pulsar = null;
        }
        if (mockZk != null) {
            mockZk.close();
        }
    }

    void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();

        // call openLedgerComplete with ledgerMock on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(Supplier.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(1, 1), null);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(byte[].class), any(AddEntryCallback.class), any());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock)
                .asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class),
                        any());

        // call deleteLedgerComplete on ledger asyncDelete
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());
    }


    @Test
    public void testEntryFilterWithSingleTag() throws Exception {
        log.info("====== Start test entry filter =====");
        // mock MessageMetadata
        MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
        msgMetadataBuilder.setPublishTime(System.currentTimeMillis());
        msgMetadataBuilder.setProducerName("pulsar.marker");
        msgMetadataBuilder.setSequenceId(0);
        Map<String, String> props = new HashMap<>();
        props.put("tag1", "TAGS");
        assertEquals(1, toKeyValueList(props).size());
        assertEquals("TAGS", toKeyValueList(props).get(0).getValue());
        assertEquals("tag1", toKeyValueList(props).get(0).getKey());
        msgMetadataBuilder.addProperties(0, toKeyValueList(props).get(0));
        MessageMetadata msgMetadata = msgMetadataBuilder.build();
        FilterContext filterContext = new FilterContext();
        filterContext.setMsgMetadata(msgMetadata);

        // mock PersistentSubscription
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock, false);
        filterContext.setSubscription(sub);

        // test: msgMetadata set properties but subscription not set subProperties , so the result is: ACCEPT.
        Entry mockEntry = mock(Entry.class);
        DefaultEntryFilter entryFilter = new DefaultEntryFilter();
        FilterResult filterResult = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult);

        // mock PersistentSubscription again
        Map<String, String> subProp = new HashMap<>();
        subProp.put("tag2", "1");
        PersistentSubscription sub1 = new PersistentSubscription(topic, "sub-2", cursorMock, false, subProp);
        filterContext.setSubscription(sub1);
        filterContext.setMsgMetadata(msgMetadata);

        // test: msgMetadata set properties and subscription set subProperties but inconsistent,
        // so the result is: REJECT.
        FilterResult filterResult1 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.REJECT, filterResult1);

        // mock PersistentSubscription again
        Map<String, String> subProp1 = new HashMap<>();
        subProp1.put("tag1", "1");
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub-3", cursorMock, false, subProp1);
        filterContext.setSubscription(sub2);
        filterContext.setMsgMetadata(msgMetadata);
        // test: msgMetadata set properties and subscription set same tag in subProperties, so the result is: ACCEPT.
        FilterResult filterResult2 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult2);

        // mock MessageMetadata again
        MessageMetadata.Builder msgMetadataBuilder1 = MessageMetadata.newBuilder();
        msgMetadataBuilder1.setPublishTime(System.currentTimeMillis());
        msgMetadataBuilder1.setProducerName("pulsar.marker1");
        msgMetadataBuilder1.setSequenceId(1);
        Map<String, String> props1 = new HashMap<>();
        props1.put("tag1", "error");
        assertEquals(1, toKeyValueList(props1).size());
        assertEquals("error", toKeyValueList(props1).get(0).getValue());
        assertEquals("tag1", toKeyValueList(props1).get(0).getKey());
        msgMetadataBuilder1.addProperties(0, toKeyValueList(props1).get(0));
        MessageMetadata msgMetadata1 = msgMetadataBuilder1.build();
        filterContext.setMsgMetadata(msgMetadata1);
        filterContext.setSubscription(sub2);
        // test: set error properties for MsgMetadata and use correct subProperties, so the result is: ACCEPT.
        FilterResult filterResult3 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult3);

        MessageMetadata.Builder msgMetadataBuilder2 = MessageMetadata.newBuilder();
        msgMetadataBuilder2.setPublishTime(System.currentTimeMillis());
        msgMetadataBuilder2.setProducerName("pulsar.marker2");
        msgMetadataBuilder2.setSequenceId(2);
        MessageMetadata msgMetadata2 = msgMetadataBuilder2.build();
        filterContext.setMsgMetadata(msgMetadata2);
        filterContext.setSubscription(sub2);
        // test: don't set properties for MsgMetadata and use correct subProperties, so the result is: ACCEPT.
        FilterResult filterResult4 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult4);
    }

    @Test
    public void testEntryFilterWithMultiTags() throws Exception {
        log.info("====== Start test entry filter =====");
        // mock MessageMetadata
        MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
        msgMetadataBuilder.setPublishTime(System.currentTimeMillis());
        msgMetadataBuilder.setProducerName("pulsar.marker");
        msgMetadataBuilder.setSequenceId(0);
        Map<String, String> props = new HashMap<>();
        props.put("tag1", "TAGS");
        props.put("tag2", "TAGS");
        assertEquals(2, toKeyValueList(props).size());
        msgMetadataBuilder.addAllProperties(buildPropertiesMap(props));
        MessageMetadata msgMetadata = msgMetadataBuilder.build();
        FilterContext filterContext = new FilterContext();
        filterContext.setMsgMetadata(msgMetadata);

        // mock PersistentSubscription
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-01", cursorMock, false);
        filterContext.setSubscription(sub);

        // test: msgMetadata set multi properties but subscription not set subProperties , so the result is: ACCEPT.
        Entry mockEntry = mock(Entry.class);
        DefaultEntryFilter entryFilter = new DefaultEntryFilter();
        FilterResult filterResult = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult);

        // mock PersistentSubscription again
        Map<String, String> subProp = new HashMap<>();
        subProp.put("tag3", "1");
        PersistentSubscription sub1 = new PersistentSubscription(topic, "sub-00", cursorMock, false, subProp);
        filterContext.setSubscription(sub1);
        filterContext.setMsgMetadata(msgMetadata);

        // test: msgMetadata set multi properties and subscription set subProperties but inconsistent,
        // so the result is: REJECT.
        FilterResult filterResult1 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.REJECT, filterResult1);

        // mock PersistentSubscription with tag1
        Map<String, String> subProp1 = new HashMap<>();
        subProp1.put("tag1", "1");
        PersistentSubscription sub2 = new PersistentSubscription(topic, "sub-02", cursorMock, false, subProp1);
        filterContext.setSubscription(sub2);
        filterContext.setMsgMetadata(msgMetadata);
        // test: msgMetadata set properties and subscription set same tag in subProperties, so the result is: ACCEPT.
        FilterResult filterResult2 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult2);

        // mock PersistentSubscription with tag2
        Map<String, String> subProp2 = new HashMap<>();
        subProp1.put("tag2", "1");
        PersistentSubscription sub3 = new PersistentSubscription(topic, "sub-03", cursorMock, false, subProp2);
        filterContext.setSubscription(sub3);
        filterContext.setMsgMetadata(msgMetadata);
        // test: msgMetadata set properties and subscription set same tag in subProperties, so the result is: ACCEPT.
        FilterResult filterResult3 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult3);

        // mock PersistentSubscription with tag2 and tag1
        Map<String, String> subProp3 = new HashMap<>();
        subProp1.put("tag2", "1");
        subProp1.put("tag1", "1");
        PersistentSubscription sub4 = new PersistentSubscription(topic, "sub-04", cursorMock, false, subProp3);
        filterContext.setSubscription(sub4);
        filterContext.setMsgMetadata(msgMetadata);
        // test: msgMetadata set properties and subscription set same tag in subProperties, so the result is: ACCEPT.
        FilterResult filterResult4 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult4);
    }

    @Test
    public void testMultiTags() throws Exception {
        // mock MessageMetadata again
        MessageMetadata.Builder msgMetadataBuilder1 = MessageMetadata.newBuilder();
        msgMetadataBuilder1.setPublishTime(System.currentTimeMillis());
        msgMetadataBuilder1.setProducerName("pulsar.marker3");
        msgMetadataBuilder1.setSequenceId(1);
        Map<String, String> props1 = new HashMap<>();
        props1.put("tag1", "error");
        props1.put("tag2", "TAGS");
        props1.put("tag3", "err");
        msgMetadataBuilder1.addAllProperties(buildPropertiesMap(props1));
        MessageMetadata msgMetadata1 = msgMetadataBuilder1.build();
        FilterContext filterContext = new FilterContext();
        filterContext.setMsgMetadata(msgMetadata1);

        // mock PersistentSubscription
        Map<String, String> subProp3 = new HashMap<>();
        subProp3.put("tag1", "1");
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub4 = new PersistentSubscription(topic, "sub-06", cursorMock, false, subProp3);
        filterContext.setSubscription(sub4);

        Entry mockEntry = mock(Entry.class);
        DefaultEntryFilter entryFilter = new DefaultEntryFilter();
        FilterResult filterResult6 = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.REJECT, filterResult6);

        Map<String, String> subProp4 = new HashMap<>();
        subProp4.put("tag2", "1");
        PersistentSubscription sub5 = new PersistentSubscription(topic, "sub-06", cursorMock, false, subProp4);
        filterContext.setSubscription(sub5);
        FilterResult filterResult = entryFilter.filterEntry(mockEntry, filterContext);
        assertEquals(FilterResult.ACCEPT, filterResult);
    }

    private List<KeyValue> buildPropertiesMap(Map<String, String> properties) {
        if (properties.isEmpty()) {
            return Collections.emptyList();
        }

        List<KeyValue> allProperties = Lists.newArrayList();
        properties.forEach((key, value) -> {
            KeyValue kv = KeyValue.newBuilder().setKey(key).setValue(value).build();
            allProperties.add(kv);
        });

        return allProperties;
    }

    private List<KeyValue> toKeyValueList(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyList();
        }

        return metadata.entrySet().stream().map(e ->
                        PulsarApi.KeyValue.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build())
                .collect(Collectors.toList());
    }

    private static final Logger log = LoggerFactory.getLogger(TestDefaultEntryFilter.class);

}
