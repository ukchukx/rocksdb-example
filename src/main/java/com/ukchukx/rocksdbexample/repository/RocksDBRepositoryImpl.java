package com.ukchukx.rocksdbexample.repository;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;
import org.springframework.util.SerializationUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Slf4j
@Repository
public class RocksDBRepositoryImpl implements KVRepository<String, Object> {

  private final static String NAME = "rocksdbfile";
  File dbDir;
  RocksDB db;

  @PostConstruct // this method will be executed after the application starts.
  void initialize() {
    RocksDB.loadLibrary();
    final Options options = new Options();
    options.setCreateIfMissing(true);
    dbDir = new File("/tmp/rocks-db", NAME);

    try {
      Files.createDirectories(dbDir.getParentFile().toPath());
      Files.createDirectories(dbDir.getAbsoluteFile().toPath());
      db = RocksDB.open(options, dbDir.getAbsolutePath());
    } catch(IOException | RocksDBException e) {
      log.error("Error initializng RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
                e.getCause(), 
                e.getMessage(), 
                e.getStackTrace());
    }

    log.info("RocksDB initialized and ready to use");
  }

  @Override
  public synchronized void save(String key, Object value) {
    log.info("saving value '{}' with key '{}'", value, key);

    try {
      db.put(key.getBytes(), SerializationUtils.serialize(value));
    } catch (RocksDBException e) {
      log.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
    }
  }

  @Override
  public synchronized Object find(String key) {
    Object value = null;

    try {
      byte[] bytes = db.get(key.getBytes());
      if(bytes != null) value = SerializationUtils.deserialize(bytes);
    } catch (RocksDBException e) {
      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", 
                key, 
                e.getCause(), 
                e.getMessage());
    }

    log.info("finding key '{}' returns '{}'", key, value);

    return value;
  }

  @Override
  public synchronized void delete(String key) {
    log.info("deleting key '{}'", key);

    try {
      db.delete(key.getBytes());
    } catch (RocksDBException e) {
      log.error("Error deleting entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
    }
  }
}