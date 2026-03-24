package redis

import (
	"context"
	"net"

	"github.com/delta10/access-operator/internal/controller"
	goredis "github.com/redis/go-redis/v9"
)

// ACLClient is the Redis surface the reconciler needs for ACL management.
type ACLClient interface {
	Ping(ctx context.Context) error
	ListUsers(ctx context.Context) ([]string, error)
	ListACL(ctx context.Context) ([]string, error)
	SetUser(ctx context.Context, username string, rules ...string) error
	DeleteUser(ctx context.Context, username string) (int64, error)
	Close() error
}

type ClientFactory func(controller.SharedConnectionDetails) ACLClient

type goRedisClient struct {
	client *goredis.Client
}

func NewClientFactory() ClientFactory {
	return func(connection controller.SharedConnectionDetails) ACLClient {
		return &goRedisClient{
			client: goredis.NewClient(&goredis.Options{
				Addr:     net.JoinHostPort(connection.Host, connection.Port),
				Username: connection.Username,
				Password: connection.Password,
			}),
		}
	}
}

func (c *goRedisClient) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *goRedisClient) ListUsers(ctx context.Context) ([]string, error) {
	return c.client.ACLUsers(ctx).Result()
}

func (c *goRedisClient) ListACL(ctx context.Context) ([]string, error) {
	return c.client.ACLList(ctx).Result()
}

func (c *goRedisClient) SetUser(ctx context.Context, username string, rules ...string) error {
	return c.client.ACLSetUser(ctx, username, rules...).Err()
}

func (c *goRedisClient) DeleteUser(ctx context.Context, username string) (int64, error) {
	return c.client.ACLDelUser(ctx, username).Result()
}

func (c *goRedisClient) Close() error {
	return c.client.Close()
}
