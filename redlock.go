package redis_lock

import (
	"context"
	"errors"
	"sync"
	"time"
)

// 红锁中每个节点默认的处理超时时间为 50 ms
const DefaultSingleLockTimeout = 50 * time.Millisecond

type RedLock struct {
	mu           sync.Mutex
	locks        []*RedisLock
	successLocks []*RedisLock
	RedLockOptions
}

// 用户在创建红锁时，需要通过传入一个 SingleNodeConf 列表的方式，显式指定每个 redis 锁节点的地址信息
func NewRedLock(key string, confs []*SingleNodeConf, opts ...RedLockOption) (*RedLock, error) {
	// 红锁必须有3 个节点以上
	if len(confs) < 3 {
		return nil, errors.New("ERROR: Can not use RedLock less than 3 nodes")
	}

	r := RedLock{}
	for _, opt := range opts {
		opt(&r.RedLockOptions)
	}

	repairRedLock(&r.RedLockOptions)
	if r.expireDuration > 0 && time.Duration(len(confs))*r.singleNodesTimeout*10 > r.expireDuration {
		// 要求所有节点累计的超时阈值要小于分布式锁过期时间的十分之一
		return nil, errors.New("ERROR: expire thresholds of single node is too long")
	}

	r.locks = make([]*RedisLock, 0, len(confs))
	r.successLocks = make([]*RedisLock, 0, len(confs))
	for _, conf := range confs {
		client := NewClient(conf.Network, conf.Address, conf.Password, conf.Opts...)
		r.locks = append(r.locks, NewRedisLock(key, client, WithExpireSeconds(int64(r.expireDuration.Seconds()))))
	}

	return &r, nil
}

func (r *RedLock) Lock(ctx context.Context) error {
	r.mu.Lock()

	successCount := 0
	for _, lock := range r.locks {
		startTime := time.Now()
		err := lock.Lock(ctx)
		cost := time.Since(startTime)
		if err == nil && cost <= r.singleNodesTimeout {
			r.successLocks = append(r.successLocks, lock)
			successCount++
		}
	}

	// 超过半数失败了
	if successCount < (len(r.locks)>>1 + 1) {
		// 对之前成功加锁的内容进行回滚
		for _, lock := range r.successLocks {
			lock.Unlock(ctx)
		}
		r.mu.Unlock()

		return errors.New("ERROR: RedLock lock failed")
	}

	r.mu.Unlock()
	return nil
}

func (r *RedLock) UnLock(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	// 只处理加锁成功的部分
	for _, lock := range r.successLocks {
		if _err := lock.Unlock(ctx); _err != nil {
			err = _err
		}
	}

	return err
}
