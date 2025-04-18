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

import java.util.Optional;
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

  public BonsaiCachedMerkleTrieLoader(final ObservableMetricsSystem metricsSystem) {
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "accountsNodes", accountNodes);
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "storageNodes", storageNodes);
  }

  public void preLoadAccount(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    CompletableFuture.runAsync(
        () -> cacheAccountNodes(worldStateKeyValueStorage, worldStateRootHash, account));
  }

  @VisibleForTesting
  public void cacheAccountNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    try {
      final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
          new StoredMerklePatriciaTrie<>(
              (location, hash) -> {
                Optional<Bytes> node =
                    getAccountStateTrieNode(worldStateKeyValueStorage, location, hash, worldStateRootHash);
                node.ifPresent(bytes -> {
                  // LOG.info("[PRELOAD ACCOUNT] - " + System.nanoTime() + ": " + Hash.hash(bytes) + " - " + location + " - " + hash);
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
      LOG.info("{}: Finished preloading account with state hash {}", System.nanoTime(), worldStateRootHash);
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
  }

  public void preLoadStorageSlot(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey,
      final Hash worldStateRootHash) {
    CompletableFuture.runAsync(
        () -> cacheStorageNodes(worldStateKeyValueStorage, account, slotKey, worldStateRootHash));
  }

  @VisibleForTesting
  public void cacheStorageNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey,
      final Hash worldStateRootHash) {
    final Hash accountHash = account.addressHash();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    try {
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
                                    worldStateKeyValueStorage, accountHash, location, hash, worldStateRootHash);
                            node.ifPresent(bytes -> {
                              // LOG.info("[PRELOAD STORAGE] - " + System.nanoTime() + ": " + Hash.hash(bytes) + " - " + location + " - " + accountHash);
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
    } finally {
      LOG.info("{}: Finished preloading storage with state hash {}", System.nanoTime(), worldStateRootHash);
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
  }

  public Optional<Bytes> getAccountStateTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      // LOG.info("[READ ACCOUNT - EMPTY] - " + System.nanoTime() + ": " + nodeHash + " - " + location);
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      final Bytes cached = accountNodes.getIfPresent(nodeHash);
      if (cached != null) {
        // LOG.info("[READ ACCOUNT - HIT] - " + System.nanoTime() + ": " + nodeHash + " - " + location);
        return Optional.of(cached);
      } else {
        // LOG.info("[READ ACCOUNT - MISS] - " + System.nanoTime() + ": " + nodeHash + " - " + location);
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
      // LOG.info("[READ STORAGE - EMPTY] - " + System.nanoTime() + ": " + nodeHash + " - " + location + " - " + accountHash);
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      final Bytes cached = storageNodes.getIfPresent(nodeHash);
      if (cached != null) {
        // LOG.info("[READ STORAGE - HIT] - " + System.nanoTime() + ": " + nodeHash + " - " + location + " - " + accountHash);
        return Optional.of(cached);
      } else {
        // LOG.info("[READ STORAGE - MISS] - " + System.nanoTime() + ": " + nodeHash + " - " + location + " - " + accountHash);
        return worldStateKeyValueStorage.getAccountStorageTrieNode(accountHash, location, nodeHash);
      }
    }
  }

  public Optional<Bytes> getAccountStateTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Bytes location,
      final Bytes32 nodeHash,
      final Hash worldStateRootHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      // LOG.info("[READ ACCOUNT - EMPTY] - " + System.nanoTime() + ": " + worldStateRootHash);
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      final Bytes cached = accountNodes.getIfPresent(nodeHash);
      if (cached != null) {
        // LOG.info("[READ ACCOUNT - HIT] - " + System.nanoTime() + ": " + worldStateRootHash);
        return Optional.of(cached);
      } else {
        // LOG.info("[READ ACCOUNT - MISS] - " + System.nanoTime() + ": " + worldStateRootHash);
        return worldStateKeyValueStorage.getAccountStateTrieNode(location, nodeHash);
      }
    }
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Hash worldStateRootHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      // LOG.info("[READ STORAGE - EMPTY] - " + System.nanoTime() + ": " + worldStateRootHash);
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      final Bytes cached = storageNodes.getIfPresent(nodeHash);
      if (cached != null) {
        // LOG.info("[READ STORAGE - HIT] - " + System.nanoTime() + ": " + worldStateRootHash);
        return Optional.of(cached);
      } else {
        // LOG.info("[READ STORAGE - MISS] - " + System.nanoTime() + ": " + worldStateRootHash);
        return worldStateKeyValueStorage.getAccountStorageTrieNode(accountHash, location, nodeHash);
      }
    }
  }
}
