# --- Build Stage ---
# 使用官方 Go 镜像作为构建环境
FROM golang:1.23-alpine AS builder

# 设置工作目录
WORKDIR /app

# 预先复制 go.mod 和 go.sum 并下载依赖项
# 这样可以利用 Docker 的层缓存，仅在依赖项更改时重新下载
COPY go.mod go.sum ./
RUN go mod download

# 复制所有源代码
COPY . .

# 构建应用程序
# CGO_ENABLED=0 确保静态链接，更适合 Alpine 镜像
# -o 指定输出的可执行文件路径
# 注意：这里的路径是根据你提供的 main.go 文件的原始路径
# 如果你已经按之前的建议重构了目录结构，请修改为 cmd/kvdb-server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app/kvdb-server ./cmd/kvdb_server/main.go


# --- Final Stage ---
# 使用一个轻量级的基础镜像
FROM alpine:latest

# 安装 ca-certificates，如果你的应用未来需要进行 https 调用可能需要
RUN apk --no-cache add ca-certificates

# 设置工作目录
WORKDIR /app

# 从构建阶段复制编译好的二进制文件
COPY --from=builder /app/kvdb-server /app/kvdb-server

# 暴露默认的 gRPC 端口（虽然 docker-compose 会覆盖，但这是个好习惯）
# 注意：Raft 端口是内部通信，通常不需要 EXPOSE
EXPOSE 50051

# 定义容器启动时执行的命令
# 这里只定义入口点，具体的参数将在 docker-compose.yml 中提供
ENTRYPOINT ["/app/kvdb-server"]