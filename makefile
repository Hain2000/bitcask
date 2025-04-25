
proto:
	rm -f ./protocol/grpc/kvdb/*.go
	protoc --proto_path=./protocol/grpc/proto --go_out=protocol/grpc/kvdb --go_opt=paths=source_relative \
	--go-grpc_out=./protocol/grpc/kvdb --go-grpc_opt=paths=source_relative \
	./protocol/grpc/proto/*.proto