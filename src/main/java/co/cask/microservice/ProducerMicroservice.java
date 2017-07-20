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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.microservice.api.AbstractMicroservice;
import co.cask.microservice.api.Emitter;
import co.cask.microservice.api.Microservice;
import co.cask.microservice.api.MicroserviceConfig;
import co.cask.microservice.api.MicroserviceContext;
import co.cask.microservice.api.MicroserviceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link Microservice} that generates numbers starting from 1 with programmable delay.
 */
@Plugin(type = Microservice.TYPE)
@Name("ProducerMicroservice")
@Description("Produces numbers starting from 1 with programmable delay.")
public class ProducerMicroservice extends AbstractMicroservice {
  private static final Logger LOG = LoggerFactory.getLogger(ProducerMicroservice.class);

  private ProducerConfig config;
  private int data;

  @Override
  public void start(MicroserviceContext context) throws MicroserviceException {
    super.start(context);
    LOG.info("Interval Delay in milliseconds : {}", config.getDelayInMillisecs());
    data = 0;
  }

  @Override
  public void produce(Emitter emitter) throws MicroserviceException {
    emitter.emit(Bytes.toBytes(++data));
    try {
      TimeUnit.MILLISECONDS.sleep(config.getDelayInMillisecs());
    } catch (InterruptedException e) {
      LOG.warn("Received an interrupt while sleeping.", e);
    }
  }

  public static class ProducerConfig extends MicroserviceConfig {

    @Nullable
    public Integer delayInMillisecs;

    public int getDelayInMillisecs() {
      if (delayInMillisecs == null || delayInMillisecs <= 0) {
        return 100;
      }
      return delayInMillisecs;
    }
  }
}
