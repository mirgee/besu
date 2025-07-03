/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.common;

import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.TreeMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

// TODO: Using TreeMap to obtain the latest updated value. This can be done differently, e.g.
// tracking
// the highest index separately.
@SuppressWarnings("NonApiType")
public class PathBasedValue<T> implements TrieLog.LogTuple<T> {
  private T prior;
  private final TreeMap<Integer, T> updatedMap;
  private boolean lastStepCleared;
  private boolean clearedAtLeastOnce;
  private boolean isEvmRead;

  // TODO: Sometimes, PathBasedValue is being used to represent per-block change,
  // sometimes a per-transaction change. Maybe they should be represented by different types.
  // Workaround for now is using -1 to index per-block change.
  public PathBasedValue(final T prior, final T updated) {
    this.prior = prior;
    this.updatedMap = new TreeMap<>();
    this.updatedMap.put(-1, updated);
    this.lastStepCleared = false;
    this.clearedAtLeastOnce = false;
    this.isEvmRead = false;
  }

  // TODO: dtto
  public PathBasedValue(final T prior, final T updated, final boolean lastStepCleared) {
    this.prior = prior;
    this.updatedMap = new TreeMap<>();
    this.updatedMap.put(-1, updated);
    this.lastStepCleared = lastStepCleared;
    this.clearedAtLeastOnce = lastStepCleared;
  }

  public PathBasedValue(final int txIndex, final T prior, final T updated) {
    this.prior = prior;
    this.updatedMap = new TreeMap<>();
    this.updatedMap.put(txIndex, updated);
    this.lastStepCleared = false;
    this.clearedAtLeastOnce = false;
    this.isEvmRead = false;
  }

  public PathBasedValue(
      final int txIndex, final T prior, final T updated, final boolean lastStepCleared) {
    this.prior = prior;
    this.updatedMap = new TreeMap<>();
    this.updatedMap.put(txIndex, updated);
    this.lastStepCleared = lastStepCleared;
    this.clearedAtLeastOnce = lastStepCleared;
  }

  public PathBasedValue(
      final int txIndex,
      final T prior,
      final T updated,
      final boolean lastStepCleared,
      final boolean clearedAtLeastOnce,
      final boolean isEvmRead) {
    this.prior = prior;
    this.updatedMap = new TreeMap<>();
    this.updatedMap.put(txIndex, updated);
    this.lastStepCleared = lastStepCleared;
    this.clearedAtLeastOnce = clearedAtLeastOnce;
    this.isEvmRead = isEvmRead;
  }

  private PathBasedValue(
      final T prior,
      final TreeMap<Integer, T> updatedMap,
      final boolean lastStepCleared,
      final boolean clearedAtLeastOnce,
      final boolean isEvmRead) {
    this.prior = prior;
    this.updatedMap = updatedMap;
    this.lastStepCleared = lastStepCleared;
    this.clearedAtLeastOnce = clearedAtLeastOnce;
    this.isEvmRead = isEvmRead;
  }

  @Override
  public T getPrior() {
    return prior;
  }

  @Override
  public T getUpdated() {
    return updatedMap.get(updatedMap.lastKey());
  }

  // TODO: Reconsider. Probably no need to expose the entire map
  public TreeMap<Integer, T> getUpdatedMap() {
    return updatedMap;
  }

  public PathBasedValue<T> setPrior(final T prior) {
    this.prior = prior;
    return this;
  }

  public PathBasedValue<T> setUpdated(final int txIndex, final T updated) {
    this.lastStepCleared = updated == null;
    if (lastStepCleared) {
      this.clearedAtLeastOnce = true;
    }
    this.updatedMap.put(txIndex, updated);
    return this;
  }

  public void setCleared() {
    this.lastStepCleared = true;
    this.clearedAtLeastOnce = true;
  }

  public void setIsEvmRead(final boolean isEvmRead) {
    this.isEvmRead = this.isEvmRead || isEvmRead;
  }

  @Override
  public boolean isLastStepCleared() {
    return lastStepCleared;
  }

  @Override
  public boolean isClearedAtLeastOnce() {
    return clearedAtLeastOnce;
  }

  public boolean isEvmRead() {
    return isEvmRead;
  }

  @Override
  public String toString() {
    return "PathBasedValue{"
        + "prior="
        + prior
        + ", updatedMap="
        + updatedMap
        + ", cleared="
        + lastStepCleared
        + ", isEvmRead="
        + isEvmRead
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PathBasedValue<?> that = (PathBasedValue<?>) o;
    return new EqualsBuilder()
        .append(lastStepCleared, that.lastStepCleared)
        .append(prior, that.prior)
        .append(updatedMap, that.updatedMap)
        .append(isEvmRead, that.isEvmRead)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(prior)
        .append(updatedMap)
        .append(lastStepCleared)
        .toHashCode();
  }

  public void importUpdatesFromSource(final PathBasedValue<T> source) {
    this.updatedMap.putAll(source.getUpdatedMap());
  }

  public PathBasedValue<T> copy() {
    return new PathBasedValue<T>(prior, updatedMap, lastStepCleared, clearedAtLeastOnce, isEvmRead);
  }
}
