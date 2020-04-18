package com.ukchukx.rocksdbexample.api;

import com.ukchukx.rocksdbexample.repository.KVRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api")
public class Api {
  private final KVRepository<String, Object> repository;

  public Api(KVRepository<String, Object> repository) {
    this.repository = repository;
  }

  // curl -iv -X POST -H "Content-Type: application/json" -d '{"bar":"baz"}' http://localhost:8080/api/foo
  @PostMapping(value = "/{key}", 
              consumes = MediaType.APPLICATION_JSON_VALUE, 
              produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Object> save(@PathVariable("key") String key, @RequestBody Object value) {
    return repository.save(key, value) 
      ? ResponseEntity.ok(value) 
      : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
  }

  // curl -iv -X GET -H "Content-Type: application/json" http://localhost:8080/api/foo
  @GetMapping(value = "/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Object> find(@PathVariable("key") String key) {
    return ResponseEntity.of(repository.find(key));
  }

  // curl -iv -X DELETE -H "Content-Type: application/json" http://localhost:8080/api/foo
  @DeleteMapping(value = "/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Object> delete(@PathVariable("key") String key) {
    return repository.delete(key) 
      ? ResponseEntity.noContent().build() 
      : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
  }
}