## 用法
### 1.单机用法
可参考 examples/basic_operation.go和*_test.go \
主要接口: db.go, iterator.go, batch.go, merge.go, redis/
### 2.部署server
#### kvdb_server
部署在一台机器上用来测试
```
docker-compose up --build
```
可以使用grpc和http访问(目前只实现了Get, Put, Delete) \
主要接口: grpc/server.go, http/http_server.go
#### redis_server
```
go run cmd/redis_server.go
```
可以使用redis-cli连接

## TODO
- [x] redis
  - [x] concurrency
  - [x] server
  - [x] server handler
- [ ] cluster
  - [x] raft
    - [x] fsm
    - [x] node
  - [x] http server
  - [x] grpc server
  - [ ] resp server
  - [x] docker-compose