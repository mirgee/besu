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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.CompactEncoding.bytesToPath;
import static org.hyperledger.besu.metrics.BesuMetricCategory.BLOCKCHAIN;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiCachedMerkleTrieLoader implements StorageSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiCachedMerkleTrieLoader.class);

  private static final int ACCOUNT_CACHE_SIZE = 100_000;
  private static final int STORAGE_CACHE_SIZE = 200_000;

  private final Cache<Bytes, Bytes> accountNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(ACCOUNT_CACHE_SIZE).build();
  private final Cache<Bytes, Bytes> storageNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(STORAGE_CACHE_SIZE).build();

  private final OperationTimer accountPreloadTimer;
  private final OperationTimer storagePreloadTimer;

  private final Counter accountCacheMissCounter;
  private final Counter storageCacheMissCounter;
  private final Counter nullCounter;

  public BonsaiCachedMerkleTrieLoader(final ObservableMetricsSystem metricsSystem) {
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "accountsNodes", accountNodes);
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "storageNodes", storageNodes);

    accountPreloadTimer =
        metricsSystem
            .createLabelledTimer(
                BLOCKCHAIN,
                "account_preload_latency_seconds",
                "Latency for preloading account nodes",
                "database")
            .labels("besu");
    storagePreloadTimer =
        metricsSystem
            .createLabelledTimer(
                BLOCKCHAIN,
                "storage_preload_latency_seconds",
                "Latency for preloading storage nodes",
                "database")
            .labels("besu");

    accountCacheMissCounter =
        metricsSystem.createCounter(
            BLOCKCHAIN, "account_cache_miss_count", "Counter for account state trie cache misses");
    storageCacheMissCounter =
        metricsSystem.createCounter(
            BLOCKCHAIN,
            "storage_cache_miss_count",
            "Counter for account storage trie cache misses");
    nullCounter =
        metricsSystem.createCounter(
            BLOCKCHAIN,
            "storage_cache_multiget_null_count",
            "Counter for account storage trie cache misses");
  }

  public void preLoadAccount(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    CompletableFuture.runAsync(() -> {
      // Set<Bytes> multiget = cacheAccountNodes(worldStateKeyValueStorage, worldStateRootHash, account);
      // Set<Bytes> trie = cacheAccountNodesTraverse(worldStateKeyValueStorage, worldStateRootHash, account);
      // Set<Bytes> naive = cacheAccountNodesNaive(worldStateKeyValueStorage, worldStateRootHash, account);
      cacheAccountNodesNaive(worldStateKeyValueStorage, worldStateRootHash, account);

      // LOG.info("[COMPARE ACCOUNT] Multiget count: {}, Trie count: {}, Naive: {}", multiget.size(), trie.size(), naive.size());
      // LOG.info("[COMPARE ACCOUNT] Multiget count: {}, Trie count: {}", multiget.size(), trie.size());

      // Set<Bytes> trieMinusMultiget = new HashSet<>(trie);
      // trieMinusMultiget.removeAll(multiget);
      //
      // Set<Bytes> multigetMinusTrie = new HashSet<>(multiget);
      // multigetMinusTrie.removeAll(trie);
      //
      // Set<Bytes> trieMinusNaive = new HashSet<>(trie);
      // trieMinusMultiget.removeAll(naive);

      // LOG.info("[ACCOUNT - TRIE MINUS MULTIGET]     {}", trieMinusMultiget.stream().map(Bytes::toHexString).toList());
      // LOG.info("[ACCOUNT - MULTIGET MINUS TRIE]     {}", multigetMinusTrie.stream().map(Bytes::toHexString).toList());
      // LOG.info("[ACCOUNT - TRIE MINUS NAIVE]     {}", trieMinusNaive.stream().map(Bytes::toHexString).toList());
    });
  }

  public void preLoadStorageSlot(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey) {
    CompletableFuture.runAsync(() -> {
      // Set<Bytes> multiget = cacheStorageNodes(worldStateKeyValueStorage, account, slotKey);
      // Set<Bytes> trie = cacheStorageNodesTraverse(worldStateKeyValueStorage, account, slotKey);
      // Set<Bytes> naive = cacheStorageNodesNaive(worldStateKeyValueStorage, account, slotKey);
      cacheStorageNodesNaive(worldStateKeyValueStorage, account, slotKey);

      // LOG.info("[COMPARE STORAGE] Multiget count: {}, Trie count: {}, Naive: {}", multiget.size(), trie.size(), naive.size());
      // LOG.info("[COMPARE STORAGE] Multiget count: {}, Trie count: {}", multiget.size(), trie.size());

      // Set<Bytes> trieMinusMultiget = new HashSet<>(trie);
      // trieMinusMultiget.removeAll(multiget);
      //
      // Set<Bytes> multigetMinusTrie = new HashSet<>(multiget);
      // multigetMinusTrie.removeAll(trie);
      //
      // LOG.info("[STORAGE - TRIE MINUS MULTIGET]     {}", trieMinusMultiget.stream().map(Bytes::toHexString).toList());
      // LOG.info("[STORAGE - MULTIGET MINUS TRIE]     {}", multigetMinusTrie.stream().map(Bytes::toHexString).toList());
      // LOG.info("[STORAGE - TRIE MINUS NAIVE]     {}", trieMinusNaive.stream().map(Bytes::toHexString).toList());
    });
  }

  @VisibleForTesting
  public Set<Bytes> cacheAccountNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    final OperationTimer.TimingContext timingContext = accountPreloadTimer.startTimer();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    Set<Bytes> visitedNodes = new HashSet<>();
    try {
      Bytes path = bytesToPath(account.addressHash());
      worldStateKeyValueStorage
          .getNearestKeyBefore(TRIE_BRANCH_STORAGE, path)
          .ifPresent(
              nk -> {
                if (nk.size() > path.size()) {
                  throw new IllegalStateException("Nearest key must be at most as long as the searched key");
                }

                String pathString = path.toHexString();
                String nkString = nk.toHexString();
                if (!pathString.regionMatches(0, nkString, 0, nkString.length())) {
                  // LOG.info("{} and {} don't share the same prefix", pathString, nkString);
                  return;
                }

                List<byte[]> inputs = new ArrayList<>(nk.size() + 1);
                for (int i = 0; i <= nk.size(); i++) {
                  Bytes slice = nk.slice(0, i);
                  inputs.add(slice.toArrayUnsafe());
                }

                // LOG.info("[PRELOAD ACCOUNT] - " + System.nanoTime() + ": " + Arrays.toString(inputs.stream().map(s -> Bytes.of(s).toHexString()).toArray()));

                List<byte[]> outputs = worldStateKeyValueStorage.getMultipleKeys(inputs);

                if (outputs.size() != inputs.size()) {
                  throw new IllegalStateException("Inputs and outputs must have equal length");
                }

                nullCounter.inc(outputs.stream().filter(e -> e == null || e.length == 0).count());

                for (int i = 0; i < outputs.size(); i++) {
                  byte[] rawNodeBytes = outputs.get(i);
                  if (rawNodeBytes != null) {
                    Bytes node = Bytes.wrap(rawNodeBytes);
                    Bytes32 nodeHash = Hash.hash(node);
                    visitedNodes.add(nodeHash);
                    accountNodes.put(nodeHash, node);
                  }
                }
              });
    } catch (Exception ex) {
      LOG.error("Error caching account nodes", ex);
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
      timingContext.close();
    }
    return visitedNodes;
  }

  @VisibleForTesting
  public Set<Bytes> cacheStorageNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey) {
    final OperationTimer.TimingContext timingContext = storagePreloadTimer.startTimer();
    final Hash accountHash = account.addressHash();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    Set<Bytes> visitedNodes = new HashSet<>();
    try {
      Bytes path = Bytes.concatenate(accountHash, bytesToPath(slotKey.getSlotHash()));
      worldStateKeyValueStorage
          .getNearestKeyBefore(TRIE_BRANCH_STORAGE, path)
          .ifPresent(
              fullKey -> {
                if (fullKey.size() < accountHash.size()) {
                  return;
                }
                Bytes nk = fullKey.slice(accountHash.size());
                List<byte[]> inputs = new ArrayList<>(nk.size() + 1);
                for (int i = 0; i <= nk.size(); i++) {
                  Bytes slice = nk.slice(0, i);
                  inputs.add(Bytes.concatenate(accountHash, slice).toArrayUnsafe());
                }

                if (nk.size() > path.size()) {
                  throw new IllegalStateException("Nearest key must be at most as long as the searched key");
                }

                String pathString = path.toHexString();
                String nkString = nk.toHexString();
                if (!pathString.regionMatches(0, nkString, 0, nkString.length())) {
                  // LOG.info("{} and {} don't share the same prefix", pathString, nkString);
                  return;
                }

                // LOG.info("[PRELOAD STORAGE] - " + System.nanoTime() + ": " + Arrays.toString(inputs.stream().map(s -> Bytes.of(s).toHexString()).toArray()));

                List<byte[]> outputs = worldStateKeyValueStorage.getMultipleKeys(inputs);

                if (outputs.size() != inputs.size()) {
                  throw new IllegalStateException("Inputs and outputs must have equal length");
                }

                nullCounter.inc(outputs.stream().filter(e -> e == null || e.length == 0).count());

                for (int i = 0; i < inputs.size(); i++) {
                  byte[] rawNodeBytes = outputs.get(i);
                  if (rawNodeBytes != null) {
                    Bytes node = Bytes.wrap(rawNodeBytes);
                    Bytes32 nodeHash = Hash.hash(node);
                    visitedNodes.add(nodeHash);
                    storageNodes.put(nodeHash, node);
                  }
                }
              });
    } catch (Exception ex) {
      LOG.error("Error caching storage nodes", ex);
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
      timingContext.close();
    }
    return visitedNodes;
  }

  @VisibleForTesting
  public Set<Bytes> cacheAccountNodesTraverse(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    Set<Bytes> visitedNodes = new HashSet<>();
    try {
      final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
          new StoredMerklePatriciaTrie<>(
              (location, hash) -> {
                Optional<Bytes> node =
                    getAccountStateTrieNode(worldStateKeyValueStorage, location, hash);
                node.ifPresent(bytes -> {
                  visitedNodes.add(Hash.hash(bytes));
                  accountNodes.put(Hash.hash(bytes), bytes);
                });
                return node;
              },
              worldStateRootHash,
              Function.identity(),
              Function.identity());
      accountTrie.get(account.addressHash());
    } catch (MerkleTrieException e) {
      // ignore exception for the cache
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
    return visitedNodes;
  }

  @VisibleForTesting
  public Set<Bytes> cacheStorageNodesTraverse(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey) {
    final Hash accountHash = account.addressHash();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    try {
      Set<Bytes> visitedNodes = new HashSet<>();
      worldStateKeyValueStorage
          .getStateTrieNode(Bytes.concatenate(accountHash, Bytes.EMPTY))
          .ifPresent(
              storageRoot -> {
                try {
                  final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
                      new StoredMerklePatriciaTrie<>(
                          (location, hash) -> {
                            Optional<Bytes> node =
                                getAccountStorageTrieNode(
                                    worldStateKeyValueStorage, accountHash, location, hash);
                            node.ifPresent(bytes -> {
                              visitedNodes.add(Hash.hash(bytes));
                              storageNodes.put(Hash.hash(bytes), bytes);
                            });
                            return node;
                          },
                          Hash.hash(storageRoot),
                          Function.identity(),
                          Function.identity());
                  storageTrie.get(slotKey.getSlotHash());
                } catch (MerkleTrieException e) {
                  // ignore exception for the cache
                }
              });
      return visitedNodes;
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
  }
  public Optional<Bytes> getAccountStateTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      // LOG.info("[READ ACCOUNT - EMPTY] - " + System.nanoTime() + ": " + nodeHash);
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      final Bytes cached = accountNodes.getIfPresent(nodeHash);
      if (cached != null) {
        // LOG.info("[READ ACCOUNT - HIT] - " + System.nanoTime() + ": " + nodeHash);
        return Optional.of(cached);
      } else {
        // LOG.info("[READ ACCOUNT - MISS] - " + System.nanoTime() + ": " + nodeHash);
        accountCacheMissCounter.inc();
        return worldStateKeyValueStorage.getAccountStateTrieNode(location, nodeHash);
      }
    }
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      // LOG.info("[READ STORAGE - EMPTY] - " + System.nanoTime() + ": " + nodeHash);
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      final Bytes cached = storageNodes.getIfPresent(nodeHash);
      if (cached != null) {
        // LOG.info("[READ STORAGE - HIT] - " + System.nanoTime() + ": " + nodeHash);
        return Optional.of(cached);
      } else {
        // LOG.info("[READ STORAGE - MISS] - " + System.nanoTime() + ": " + nodeHash);
        storageCacheMissCounter.inc();
        return worldStateKeyValueStorage.getAccountStorageTrieNode(accountHash, location, nodeHash);
      }
    }
  }

  @VisibleForTesting
  public Set<Bytes> cacheAccountNodesNaive(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    final OperationTimer.TimingContext timingContext = accountPreloadTimer.startTimer();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    Set<Bytes> visitedNodes = new HashSet<>();
    try {
      Bytes path = bytesToPath(account.addressHash());
      int size = path.size();
      List<byte[]> inputs = new ArrayList<>(size);
      for (int i = 0; i < path.size(); i++) {
        Bytes slice = path.slice(0, i);
        inputs.add(slice.toArrayUnsafe());
      }

      // LOG.info("[PRELOAD ACCOUNT] - " + System.nanoTime() + ": " + Arrays.toString(inputs.stream().map(s -> Bytes.of(s).toHexString()).toArray()));

      List<byte[]> outputs = worldStateKeyValueStorage.getMultipleKeys(inputs);

      if (outputs.size() != inputs.size()) {
        throw new IllegalStateException("Inputs and outputs must have equal length");
      }

      nullCounter.inc(outputs.stream().filter(e -> e == null || e.length == 0).count());

      for (int i = 0; i < outputs.size(); i++) {
        byte[] rawNodeBytes = outputs.get(i);
        if (rawNodeBytes != null) {
          Bytes node = Bytes.wrap(rawNodeBytes);
          Bytes32 nodeHash = Hash.hash(node);
          visitedNodes.add(nodeHash);
          accountNodes.put(nodeHash, node);
        }
      }
    } catch (Exception ex) {
      LOG.error("Error caching account nodes", ex);
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
      timingContext.close();
    }
    return visitedNodes;
  }

  @VisibleForTesting
  public Set<Bytes> cacheStorageNodesNaive(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey) {
    final OperationTimer.TimingContext timingContext = storagePreloadTimer.startTimer();
    final Hash accountHash = account.addressHash();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    Set<Bytes> visitedNodes = new HashSet<>();
    try {
      Bytes path = bytesToPath(slotKey.getSlotHash());
      int size = path.size();
      List<byte[]> inputs = new ArrayList<>(size);
      for (int i = 0; i < path.size(); i++) {
        Bytes slice = path.slice(0, i);
        inputs.add(Bytes.concatenate(accountHash, slice).toArrayUnsafe());
      }

      // LOG.info("[PRELOAD STORAGE] - " + System.nanoTime() + ": " + Arrays.toString(inputs.stream().map(s -> Bytes.of(s).toHexString()).toArray()));

      List<byte[]> outputs = worldStateKeyValueStorage.getMultipleKeys(inputs);

      if (outputs.size() != inputs.size()) {
        throw new IllegalStateException("Inputs and outputs must have equal length");
      }

      nullCounter.inc(outputs.stream().filter(e -> e == null || e.length == 0).count());

      for (int i = 0; i < inputs.size(); i++) {
        byte[] rawNodeBytes = outputs.get(i);
        if (rawNodeBytes != null) {
          Bytes node = Bytes.wrap(rawNodeBytes);
          Bytes32 nodeHash = Hash.hash(node);
          visitedNodes.add(nodeHash);
          storageNodes.put(nodeHash, node);
        }
      }
    } catch (Exception ex) {
      LOG.error("Error caching storage nodes", ex);
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
      timingContext.close();
    }
    return visitedNodes;
  }
}
