/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.microservice;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.microservice.api.Artifact;
import co.cask.microservice.api.Channel;
import co.cask.microservice.api.ChannelType;
import co.cask.microservice.api.Endpoints;
import co.cask.microservice.api.Microservice;
import co.cask.microservice.api.MicroserviceConfiguration;
import co.cask.microservice.api.MicroserviceDefinition;
import co.cask.microservice.api.Plugin;
import co.cask.microservice.channel.tms.TMSChannelManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MicroserviceTest extends TestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(
    co.cask.cdap.common.conf.Constants.Explore.EXPLORE_ENABLED, false);

  private static final ArtifactId APP_ARTIFACT = new ArtifactId(NamespaceId.DEFAULT.getNamespace(),
                                                                "microservice-artifact", "1.0.0");
  private static final ArtifactId PLUGIN_ARTIFACT = new ArtifactId(NamespaceId.DEFAULT.getNamespace(),
                                                                   "micro-plugins", "1.0.0");

  private static final ApplicationId PRODUCER_APP_ID = new ApplicationId(
    NamespaceId.DEFAULT.getNamespace(), "microservice-producer-app");

  private static final ApplicationId PROCESSOR_APP_ID = new ApplicationId(
    NamespaceId.DEFAULT.getNamespace(), "microservice-processor-app");

  @BeforeClass
  public static void setup() throws Exception {
    addAppArtifact(APP_ARTIFACT, MicroserviceApp.class, Microservice.class.getPackage().getName());
    addPluginArtifact(PLUGIN_ARTIFACT, APP_ARTIFACT, ConsumerMicroservice.class, ProcessorMicroservice.class,
                      ProducerMicroservice.class);
  }

  @Test
  public void testMicroservice() throws Exception {
    getMessagingAdmin(NamespaceId.DEFAULT).createTopic("raw");
    getMessagingAdmin(NamespaceId.DEFAULT).createTopic("processed");
    Channel rawChannel  = new Channel(ChannelType.TMS, "raw", ImmutableMap.of(
      TMSChannelManager.TOPIC_NAME, "raw", TMSChannelManager.NAMESPACE_NAME, "default"));
    Channel processedChannel = new Channel(ChannelType.TMS, "processed", ImmutableMap.of(
      TMSChannelManager.NAMESPACE_NAME, "default", TMSChannelManager.TOPIC_NAME, "processed"));

    Endpoints producerEndpoints = new Endpoints(ImmutableList.<Channel>of(), ImmutableList.of(rawChannel), 100);
    Endpoints processorEndpoints = new Endpoints(ImmutableList.of(rawChannel), ImmutableList.of(processedChannel), 100);

    MicroserviceConfiguration producerConfig = new MicroserviceConfiguration(
      1, 10, ImmutableMap.<String, String>of(), 1, 100, producerEndpoints);

    MicroserviceConfiguration processorConfig = new MicroserviceConfiguration(
      1, 10, ImmutableMap.<String, String>of(), 1, 100, processorEndpoints);

    MicroserviceDefinition producerDefinition = new MicroserviceDefinition(
      1, "producer", "Produces Numbers", new Plugin("ProducerMicroservice", new Artifact(
        PLUGIN_ARTIFACT.getArtifact(), PLUGIN_ARTIFACT.getVersion(), ArtifactScope.USER.name())), producerConfig);

    MicroserviceDefinition processorDefinition = new MicroserviceDefinition(
      1, "processor", "Process Numbers", new Plugin("ProcessorMicroservice", new Artifact(
        PLUGIN_ARTIFACT.getArtifact(), PLUGIN_ARTIFACT.getVersion(), ArtifactScope.USER.name())), processorConfig);

    ApplicationManager producerManager = deployApplication(PRODUCER_APP_ID, new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT.getArtifact(), APP_ARTIFACT.getVersion()), producerDefinition));

    ApplicationManager processorManager = deployApplication(PROCESSOR_APP_ID, new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT.getArtifact(), APP_ARTIFACT.getVersion()), processorDefinition));


    WorkerManager producerWorker = producerManager.getWorkerManager("microservice");
    WorkerManager processorWorker = processorManager.getWorkerManager("microservice");

    producerWorker.start();
    processorWorker.start();
    producerWorker.waitForStatus(true);
    processorWorker.waitForStatus(true);

    TimeUnit.SECONDS.sleep(5);

    producerWorker.stop();
    processorWorker.stop();
    producerWorker.waitForStatus(false);
    processorWorker.waitForStatus(false);

    MessageFetcher fetcher = getMessagingContext().getMessageFetcher();
    CloseableIterator<Message> iterator = fetcher.fetch(NamespaceId.DEFAULT.getNamespace(), "processed", 10, 0);
    int count = 0;
    while (iterator.hasNext()) {
      Message message = iterator.next();
      int data = Bytes.toInt(message.getPayload());
      Assert.assertTrue(data % 2 == 0);
      count++;
    }
    Assert.assertTrue(count > 0);
    iterator.close();
  }
}
