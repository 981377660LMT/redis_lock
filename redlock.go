package redis_lock

import (
	"context"
	"errors"
	"time"
)

// 红锁中每个节点默认的处理超时时间为 50 ms
const DefaultSingleLockTimeout = 50 * time.Millisecond

type RedLock struct {
	locks []*RedisLock
	RedLockOptions
}

// 用户在创建红锁时，需要通过传入一个 SingleNodeConf 列表的方式，显式指定每个 redis 锁节点的地址信息
func NewRedLock(key string, confs []*SingleNodeConf, opts ...RedLockOption) (*RedLock, error) {
	// 3 个节点以上，红锁才有意义
	if len(confs) < 3 {
		return nil, errors.New("can not use redLock less than 3 nodes")
	}

	r := RedLock{}
	for _, opt := range opts {
		opt(&r.RedLockOptions)
	}

	repairRedLock(&r.RedLockOptions)
	if r.expireDuration > 0 && time.Duration(len(confs))*r.singleNodesTimeout*10 > r.expireDuration {
		// 要求所有节点累计的超时阈值要小于分布式锁过期时间的十分之一(不能把太多时间浪费在网络通信上，保证用户取得红锁后有充足的时间处理业务逻辑)
		// expireDuration <=0 为看门狗模式
		return nil, errors.New("expire thresholds of single node is too long")
	}

	r.locks = make([]*RedisLock, 0, len(confs))
	for _, conf := range confs {
		client := NewClient(conf.Network, conf.Address, conf.Password, conf.Opts...)
		r.locks = append(r.locks, NewRedisLock(key, client, WithExpireSeconds(int64(r.expireDuration.Seconds()))))
	}

	return &r, nil
}

func (r *RedLock) Lock(ctx context.Context) error {
	var successCnt int
	for _, lock := range r.locks {
		startTime := time.Now()
		// 保证请求耗时在指定阈值以内
		_ctx, cancel := context.WithTimeout(ctx, r.singleNodesTimeout)
		defer cancel()
		err := lock.Lock(_ctx)
		cost := time.Since(startTime)
		if err == nil && cost <= r.singleNodesTimeout {
			successCnt++
		}
	}

	if successCnt < len(r.locks)>>1+1 {
		// 倘若加锁失败，则进行解锁操作
		_ = r.Unlock(ctx)
		return errors.New("lock failed")
	}

	return nil
}

// 解锁时，对所有节点广播解锁
func (r *RedLock) Unlock(ctx context.Context) error {
	var err error
	for _, lock := range r.locks {
		if _err := lock.Unlock(ctx); _err != nil {
			if err == nil {
				err = _err
			}
		}
	}
	return err
}
