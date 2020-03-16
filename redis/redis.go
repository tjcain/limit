package redis

import (
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/tjcain/limit"
)

// Bucket implements the limit.Bucket interface backed by a
// redis persistance layer.
type Bucket struct {
	// redis transactions are sequential, so perhaps the
	// mutex protecting values here is not required.
	sync.Mutex

	rdb *redis.Client

	// key should be shared between replicas of the program
	// using this rate limiter.
	key      string
	capacity int
	space    int
	reset    time.Time
	rate     time.Duration
}

// Consume implements the limit.Bucket interface.
func (b *Bucket) Consume(amt int) (limit.State, error) {
	// Leverage redis optimistic transactions to protect
	// from concurrent writes.
	err := b.rdb.Watch(func(tx *redis.Tx) error {
		n, err := tx.Get(b.key).Int()
		if err != nil {
			if err == redis.Nil {
				// set key:
				b.rdb.Set(b.key, 0, b.rate)
			} else {
				return err
			}
		}
		if n >= b.capacity {
			b.drain() // attempt drain on exit.
			return limit.ErrToManyRequests
		}

		var count int64
		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			count, err = tx.IncrBy(b.key, int64(amt)).Result()
			return err
		})

		// check for bucket overflow.
		if int(count) > b.capacity {
			b.drain() // attempt drain on exit.
			return limit.ErrToManyRequests
		}

		b.Lock()
		b.space = b.capacity - int(count)
		b.Unlock()

		b.drain()

		return nil
	}, b.key)

	return b.state(), err
}

func (b *Bucket) drain() error {
	return b.rdb.Watch(func(tx *redis.Tx) error {

		pttl, err := b.rdb.Do("PTTL", b.key).Int()
		if err != nil && err != redis.Nil {
			return err
		}

		if pttl < 1 || err == redis.Nil {
			// todo, check if I need to reset here.
			_, err := b.rdb.Set(b.key, 0, b.rate).Result()
			if err != nil {
				return err
			}
		}

		b.Lock()
		defer b.Unlock()
		b.reset = time.Now().Add(time.Duration(pttl) * time.Millisecond)
		return nil
	}, b.key)
}

func (b *Bucket) state() limit.State {
	return limit.State{
		Capacity: b.capacity,
		Space:    b.space,
		Reset:    b.reset,
	}
}

func NewBucket(cfg Config) (*Bucket, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPwd,
		DB:       cfg.RedisDB,
	})
	_, err := rdb.Ping().Result()
	if err != nil {
		return &Bucket{}, err
	}

	if cfg.AppKey == "" {
		return &Bucket{}, errors.New("please provide appkey")
	}

	return &Bucket{
		rdb:      rdb,
		capacity: cfg.RequestLimit,
		rate:     cfg.LimitDuration,
		key:      cfg.AppKey,
	}, nil

}

// Config represents the configuration for a redis backed leaky-bucket
// http round trip rate limiter.
type Config struct {
	// AppKey identifies the application using the rate limiter.
	// It should be the same value for all instances.
	AppKey string

	RedisAddr string
	RedisPwd  string
	RedisDB   int // default 0

	RequestLimit  int
	LimitDuration time.Duration
}
