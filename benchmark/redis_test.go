package benchmark

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client
var ctx = context.Background()

func openRedisNoPersist() func() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // 连接本地 Redis
		Password: "",               // 无密码
		DB:       0,                // 默认 DB
	})

	// 禁用持久化（需手动在 Redis 配置中关闭 appendonly）
	rdb.FlushDB(ctx) // 清空数据库，确保测试环境干净

	return func() {
		_ = rdb.Close()
	}
}

func BenchmarkRedisNoPersist(b *testing.B) {
	closer := openRedisNoPersist()
	defer closer()

	b.Run("put", benchmarkRedisPut)
	b.Run("get", benchmarkRedisGet)
	b.Run("delete", benchmarkRedisDelete)
}

func benchmarkRedisPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	pipe := rdb.Pipeline() // 使用 Pipeline 批量写入
	for i := 0; i < b.N; i++ {
		pipe.Set(ctx, getKey(i), getValue(), 0) // 0 表示无过期时间
	}
	_, _ = pipe.Exec(ctx)
}

func benchmarkRedisGet(b *testing.B) {
	// 预写入数据
	pipe := rdb.Pipeline()
	for i := 0; i < b.N; i++ {
		pipe.Set(ctx, getKey(i), getValue(), 0)
	}
	_, _ = pipe.Exec(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	// 使用 MGET 批量获取
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = getKey(rand.Intn(b.N))
	}

	_, _ = rdb.MGet(ctx, keys...).Result()
}

func benchmarkRedisDelete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	pipe := rdb.Pipeline()
	for i := 0; i < b.N; i++ {
		pipe.Del(ctx, getKey(i))
	}
	_, _ = pipe.Exec(ctx)
}

// 生成测试键值
func getKey(i int) string {
	return "key_" + strconv.Itoa(i)
}

func getValue() string {
	return "value_" + strconv.Itoa(rand.Intn(100000))
}
