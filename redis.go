package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type Option struct {
	// with port
	Host     string `env:""`
	Password string `env:""`
	Db       int    `env:""`
	PoolSize int    `env:""`
}

func (c *Option) SetDefaults() {
	if c.PoolSize == 0 {
		c.PoolSize = 10
	}
}

type Redis struct {
	ctx  context.Context
	Pool *redis.Client
}

func Init(ctx context.Context, c *Option) *Redis {
	r := &Redis{}
	r.ctx = ctx

	c.SetDefaults()

	r.Pool = redis.NewClient(&redis.Options{
		Addr:               c.Host,
		Password:           c.Password, // no password set
		DB:                 c.Db,       // use default DB
		PoolSize:           c.PoolSize,
		PoolTimeout:        time.Duration(time.Second * 5),
		IdleTimeout:        time.Duration(time.Second * 2),
		IdleCheckFrequency: time.Duration(time.Second * 5),
	})

	err := r.Pool.Info(ctx).Err()
	if err != nil {
		panic(err)
	}

	return r
}

func (r *Redis) Info(key string) (value []byte, err error) {

	value, err = r.Pool.Info(r.ctx, key).Bytes()
	if err == redis.Nil {
		return nil, err
	} else if err != nil {
		return nil, err
	}
	return
}

/*
	1  exist  0 not exist
*/
func (r *Redis) IsExist(key string) bool {
	ex, _ := r.Pool.Exists(r.ctx, key).Result()
	return ex > 0
}

func (r *Redis) SAdd(key string, values ...interface{}) (err error) {
	return r.Pool.SAdd(r.ctx, key, values).Err()
}

func (r *Redis) SMember(key string) (members []string, err error) {
	return r.Pool.SMembers(r.ctx, key).Result()
}

func (r *Redis) SRem(key string, value string) (err error) {
	return r.Pool.SRem(r.ctx, key, value).Err()
}

func (r *Redis) AddSetKey(key string, value []byte, expire int) (err error) {

	if expire <= 0 {
		err = r.Pool.Set(r.ctx, key, value, time.Duration(0)).Err()
	} else {
		err = r.Pool.Set(r.ctx, key, value, time.Duration(expire)*time.Second).Err()
	}

	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) SetKeepTTL(key string, value []byte) (err error) {

	err = r.Pool.Set(r.ctx, key, value, redis.KeepTTL).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) Set(key string, value interface{}, expire int) (err error) {
	if expire <= 0 {
		err = r.Pool.Set(r.ctx, key, value, time.Duration(-1)).Err()
	} else {
		err = r.Pool.Set(r.ctx, key, value, time.Duration(expire)*time.Second).Err()
	}

	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) Get(key string) interface{} {
	value, err := r.Pool.Get(r.ctx, key).Result()
	if err != nil {
		return nil
	}
	return value
}

func (r *Redis) GetByte(key string) (value []byte, err error) {

	value, err = r.Pool.Get(r.ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	return
}

func (r *Redis) MGet(key ...string) (values []interface{}, err error) {
	values, err = r.Pool.MGet(r.ctx, key...).Result()
	if err != nil {
		return nil, err
	}
	return
}

func (r *Redis) HGetAll(key string) (map[string]string, error) {
	values, err := r.Pool.HGetAll(r.ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (r *Redis) HSet(key string, values map[string]interface{}) error {
	return r.Pool.HSet(r.ctx, key, values).Err()
}

func (r *Redis) HGet(key, field string) ([]byte, error) {
	return r.Pool.HGet(r.ctx, key, field).Bytes()
}

func (r *Redis) HExist(key, field string) (bool, error) {
	return r.Pool.HExists(r.ctx, key, field).Result()
}

func (r *Redis) HMGet(key string, fields ...string) ([]interface{}, error) {
	return r.Pool.HMGet(r.ctx, key, fields...).Result()
}

func (r *Redis) HKeys(key string) ([]string, error) {
	return r.Pool.HKeys(r.ctx, key).Result()
}

func (r *Redis) Delete(key string) (err error) {
	return r.Pool.Del(r.ctx, key).Err()
}

func (r *Redis) KeyList(key string, count int64) (keys []string, err error) {
	cursor := uint64(0)
	for {
	Loop:
		keyList, nextIndex, err := r.Scan(key, cursor, count)
		if err != nil {
			return nil, err
		}
		for index := range keyList {
			keys = append(keys, keyList[index])
		}
		if nextIndex == 0 {
			return keys, nil
		}
		cursor = nextIndex
		goto Loop
	}
}

func (r *Redis) Scan(key string, cursor uint64, count int64) ([]string, uint64, error) {
	return r.Pool.Scan(r.ctx, cursor, key, count).Result()
}

func (r *Redis) SetNX(key string, value []byte, expire int) (success bool, err error) {

	if expire <= 0 {
		success, err = r.Pool.SetNX(r.ctx, key, value, time.Duration(0)).Result()
	} else {
		success, err = r.Pool.SetNX(r.ctx, key, value, time.Duration(expire)*time.Second).Result()
	}

	if err != nil {
		return false, err
	}
	return success, nil
}

func (r *Redis) SetEX(key string, value []byte, expire int) (err error) {

	if expire <= 0 {
		err = r.Pool.SetEX(r.ctx, key, value, time.Duration(-1)).Err()
	} else {
		err = r.Pool.SetEX(r.ctx, key, value, time.Duration(expire)*time.Second).Err()
	}

	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) Lock(key string, nowTime int64, expiresIn time.Duration) (bool, error) {
	success, err := r.Pool.SetNX(r.ctx, key, nowTime, expiresIn).Result()
	if err != nil {
		return false, err
	}
	return success, nil
}

func (r *Redis) Incr(key string) (int64, error) {
	v, err := r.Pool.Incr(r.ctx, key).Result()
	if err != nil {
		return v, err
	}
	return v, nil
}

func (r *Redis) Expire(key string, expireTime time.Duration) error {
	return r.Pool.Expire(r.ctx, key, expireTime).Err()
}

func (r *Redis) GetInt64(key string) (value int64, err error) {
	value, err = r.Pool.Get(r.ctx, key).Int64()
	return
}

func (r *Redis) SetInt64(key string, value int64, expire int64) error {
	return r.Pool.Set(r.ctx, key, value, time.Duration(expire)*time.Second).Err()
}

func (r *Redis) TTL(key string) (time.Duration, error) {
	return r.Pool.TTL(r.ctx, key).Result()
}

func (r *Redis) AddMessage(stream string, data []byte) error {
	err := r.Pool.XAdd(r.ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "",
		Values: map[string]interface{}{
			"data": data,
		},
	}).Err()

	return err

}

func (r *Redis) CreateGroup(stream string, group string) error {
	groups, err := r.Pool.XInfoGroups(r.ctx, stream).Result()
	if err != nil {
		if err.Error() == "ERR no such key" {
			return r.Pool.XGroupCreateMkStream(r.ctx, stream, group, "0").Err()
		}
		return err
	}

	var found bool

	for _, g := range groups {
		if g.Name == group {
			found = true
		}
	}

	if found {
		return nil
	}

	return r.Pool.XGroupCreateMkStream(r.ctx, stream, group, "0").Err()
}

func (r *Redis) ReadGroup(stream string, group string, consumerID string, count int64) ([]redis.XMessage, error) {
	streams, err := r.Pool.XReadGroup(r.ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerID,
		Streams:  []string{stream, ">"},
		Count:    count,
	}).Result()

	if err != nil {
		return nil, err
	}

	msgs := make([]redis.XMessage, 0)

	for _, s := range streams {
		for _, msg := range s.Messages {
			msgs = append(msgs, msg)
		}
	}

	return msgs, nil
}

func (r *Redis) ReadGroupBlock(stream string, group string, consumerID string, count int64, block time.Duration) ([]redis.XMessage, error) {
	streams, err := r.Pool.XReadGroup(r.ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerID,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil && err != redis.Nil {
		return nil, err
	}

	if err == redis.Nil {
		return nil, nil
	}

	msgs := make([]redis.XMessage, 0)

	for _, s := range streams {
		for _, msg := range s.Messages {
			msgs = append(msgs, msg)
		}
	}

	return msgs, nil
}

func (r *Redis) DeleteMessage(stream string, ids ...string) error {
	return r.Pool.XDel(r.ctx, stream, ids...).Err()
}

func (r *Redis) AckMessage(stream string, group string, ids ...string) error {
	return r.Pool.XAck(r.ctx, stream, group, ids...).Err()
}
