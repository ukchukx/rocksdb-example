package com.ukchukx.rocksdbexample.repository;

public interface KVRepository<K, V> {
  void save(K key, V value);
  V find(K key);
  void delete(K key);
}