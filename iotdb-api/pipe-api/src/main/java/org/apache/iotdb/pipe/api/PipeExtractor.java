/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.pipe.api;

import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

/**
 * {@link PipeExtractor} (Deprecated since v1.3.0, renamed to {@link PipeSource})
 *
 * <p>{@link PipeExtractor} is responsible for capturing {@link Event}s from sources.
 *
 * <p>Various data sources can be supported by implementing different {@link PipeExtractor} classes.
 *
 * <p>The lifecycle of a {@link PipeExtractor} is as follows:
 *
 * <ul>
 *   <li>When a collaboration task is created, the KV pairs of `WITH EXTRACTOR` clause in SQL are
 *       parsed and the validation method {@link PipeExtractor#validate(PipeParameterValidator)}
 *       will be called to validate the {@link PipeParameters}.
 *   <li>Before the collaboration task starts, the method {@link
 *       PipeExtractor#customize(PipeParameters, PipeExtractorRuntimeConfiguration)} will be called
 *       to config the runtime behavior of the {@link PipeExtractor}.
 *   <li>Then the method {@link PipeExtractor#start()} will be called to start the {@link
 *       PipeExtractor}.
 *   <li>While the collaboration task is in progress, the method {@link PipeExtractor#supply()} will
 *       be called to capture {@link Event}s from sources and then the {@link Event}s will be passed
 *       to the {@link PipeProcessor}.
 *   <li>The method {@link PipeExtractor#close()} will be called when the collaboration task is
 *       cancelled (the `DROP PIPE` command is executed).
 * </ul>
 */
@Deprecated // since v1.3.0, renamed to PipeSource
public interface PipeExtractor extends PipePlugin {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeExtractor#customize(PipeParameters, PipeExtractorRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeParameterValidator validator) throws Exception;

  /**
   * This method is mainly used to customize {@link PipeExtractor}. In this method, the user can do
   * the following things:
   *
   * <ul>
   *   <li>Use {@link PipeParameters} to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in {@link PipeExtractorRuntimeConfiguration}.
   * </ul>
   *
   * <p>This method is called after the method {@link
   * PipeExtractor#validate(PipeParameterValidator)} is called.
   *
   * @param parameters used to parse the input {@link PipeParameters} entered by the user
   * @param configuration used to set the required properties of the running {@link PipeExtractor}
   * @throws Exception the user can throw errors if necessary
   */
  void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception;

  /**
   * Start the {@link PipeExtractor}. After this method is called, {@link Event}s should be ready to
   * be supplied by {@link PipeExtractor#supply()}. This method is called after {@link
   * PipeExtractor#customize(PipeParameters, PipeExtractorRuntimeConfiguration)} is called.
   *
   * @throws Exception the user can throw errors if necessary
   */
  void start() throws Exception;

  /**
   * Supply single {@link Event} from the {@link PipeExtractor} and the caller will send the {@link
   * Event} to the {@link PipeProcessor}. This method is called after {@link PipeExtractor#start()}
   * is called.
   *
   * @return the {@link Event} to be supplied. the {@link Event} may be {@code null} if the {@link
   *     PipeExtractor} has no more {@link Event}s at the moment, but the {@link PipeExtractor} is
   *     still running for more {@link Event}s.
   * @throws Exception the user can throw errors if necessary
   */
  Event supply() throws Exception;
}
