<?php

namespace Models;

use Models\Model;

Class EventMarket extends Model
{
    protected static $table = 'event_markets';

    public static function updateDataByEventMarketId($connection, $eventMarketId, $data)
    {
		return static::update($connection, $data, [
            'id' => $marketId
        ]);
    }

    public static function getDataByBetIdentifier($connection, $betIdentifier) {
        $sql = "SELECT * FROM " . static::$table . " WHERE bet_identifier = '{$betIdentifier}' LIMIT 1";
		return $connection->query($sql);
    }

    public static function getActiveEventMarkets($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE deleted_at is null";
		return $connection->query($sql);
    }
}