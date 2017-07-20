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
import co.cask.microservice.api.EventContext;
import co.cask.microservice.api.Microservice;
import co.cask.microservice.api.MicroserviceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Microservice} that simply logs the incoming integer data.
 */
@Plugin(type = Microservice.TYPE)
@Name("ConsumerMicroservice")
@Description("Consumes and logs received integer data.")
public class ConsumerMicroservice extends AbstractMicroservice {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerMicroservice.class);

  @Override
  public void consume(byte[] input, EventContext context) throws MicroserviceException {
    LOG.info("Received data {}", Bytes.toInt(input));
  }
}
