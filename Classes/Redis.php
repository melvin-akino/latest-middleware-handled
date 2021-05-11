<?php

namespace Classes;

use Exception;
use Predis\Client as Redis;

class RedisService
{
    private $redis;

     /**
     * @param RedisServer $redis
     * @param string $redisHost
     * @param string $redisPort
     */
    public function __construct()
    {
        $this->redis = new Redis([
            'scheme' => 'tcp',
            'host'   => getenv('REDIS_HOST', '127.0.0.1'),
            'port'   => getenv('REDIS_PORT', '6379'),
        ]);
    }

    public function lpush($key, $value) 
    {
        $this->redis->lpush($key, $value);
    }

    public function exists($key, $value) 
    {
        return $this->redis->exists($key, $value);
    }

    /**
     * Close connection when Redis service is destroyed
     */
    public function __destruct()
    {
        $this->redis->close();
        $this->redis = null;
    }
}