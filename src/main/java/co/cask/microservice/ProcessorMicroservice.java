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
import co.cask.microservice.api.EventContext;
import co.cask.microservice.api.Microservice;
import co.cask.microservice.api.MicroserviceConfig;
import co.cask.microservice.api.MicroserviceContext;
import co.cask.microservice.api.MicroserviceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * A {@link Microservice} that receives an integer and emits it only if it is a multiple of a certain integer which
 * is supplied through configuration.
 */
@Plugin(type = Microservice.TYPE)
@Name("ProcessorMicroservice")
@Description("Receives and emits that integer only if it is a multiple of a certain number.")
public class ProcessorMicroservice extends AbstractMicroservice {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessorMicroservice.class);
  private ProcessConfig config;

  @Override
  public void start(MicroserviceContext context) throws MicroserviceException {
    super.start(context);
    LOG.info("Will be checking whether a received number is a multiple of : {}", config.getMultipleOf());
  }

  @Override
  public void process(byte[] input, EventContext context, Emitter emitter) throws MicroserviceException {
    int inputData = Bytes.toInt(input);
    if (inputData % config.getMultipleOf() == 0) {
      emitter.emit(input);
    }
  }

  public static class ProcessConfig extends MicroserviceConfig {

    @Nullable
    public Integer multipleOf;

    public int getMultipleOf() {
      if (multipleOf == null || multipleOf <= 0) {
        return 2;
      }
      return multipleOf;
    }
  }
}
