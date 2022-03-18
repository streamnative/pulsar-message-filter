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

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.collections.MapUtils;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

/**
 * Tag message impl.
 * Tag limit as follows:
 *  1. When sending a message, the tag must be set in Properties Map.
 *  2. When setting tags in Properties in the sending phase, the key of Properties Map must be: TAGS.
 *  3. When sending a message, you need to disable batch, which is enabled by default.
 *  4. When consuming messages, the tag must be set in the SubscriptionProperties field, and the
 *     key of the SubscriptionProperties Map is the tag field, and the value is the version information.
 */
@Slf4j
public class DefaultEntryFilter implements EntryFilter {

    public static final String SYSTEM_PROPERTY_TAGS = "TAGS";

    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {

        PersistentSubscription subscription = (PersistentSubscription) context.getSubscription();
        Map<String, String> subProperties = subscription.getSubscriptionProperties();
        MessageMetadata msgMetadata = context.getMsgMetadata();

        // When subProperties is null, means users not set any tags for subscription,
        // now let us deliver the message to consumer.
        if (subProperties == null || MapUtils.isEmpty(subProperties)) {
            return FilterResult.ACCEPT;
        }

        // producer -> message metadata:
        // {tag1, SYSTEM_PROPERTY_TAGS}
        // {tag2, SYSTEM_PROPERTY_TAGS}
        // {tag3, SYSTEM_PROPERTY_TAGS}

        // consumer -> subProperties:
        // {tag1, version-0}

        // When msgMetadataProperties is null, means the entry not contains any message properties,
        // now let us deliver the message to consumer.
        if (msgMetadata.getPropertiesCount() <= 0) {
            return FilterResult.ACCEPT;
        }

        // In tag message, we should ignore batch message.
        // When users use batch, we should deliver the message to consumer.
        if (!msgMetadata.hasNumMessagesInBatch() || msgMetadata.getNumMessagesInBatch() == 1) {
            List<KeyValue> keyValues = msgMetadata.getPropertiesList();
            FilterResult tagCheckFlag = FilterResult.ACCEPT;
            for (KeyValue keyValue : keyValues) {
                if (keyValue.getValue().equals(SYSTEM_PROPERTY_TAGS)) {
                    if (subProperties.containsKey(keyValue.getKey())) {
                        return FilterResult.ACCEPT;
                    } else {
                        // Users set tag, but it’s inconsistent with expectations, skip the message.
                        tagCheckFlag = FilterResult.REJECT;
                    }
                }

                // no tag messages properties, please ignore and deliver the message to consumer.
            }

            return tagCheckFlag;
        } else {
            // Tag message not support batch, let us deliver the message to consumer.
            return FilterResult.ACCEPT;
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
