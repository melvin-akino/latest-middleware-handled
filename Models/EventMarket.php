<?php

namespace Models;

use Models\Model;

class EventMarket extends Model
{
    protected static $table = 'event_markets';

    public static function updateDataByEventMarketId($connection, $eventMarketId, $data)
    {
        return static::update($connection, $data, [
            'id' => $eventMarketId
        ]);
    }

    public static function getDataByBetIdentifier($connection, $betIdentifier)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE bet_identifier = '{$betIdentifier}' LIMIT 1";
        return $connection->query($sql);
    }

    public static function getActiveEventMarkets($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }

    public static function getAllUnmatchedMarket($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE id NOT IN (SELECT event_market_id FROM event_market_groups)";
        var_dump($sql);
        return $connection->query($sql);
    }

    public static function getMarketsByEventId($connection, $eventId)
    {
        $sql = "SELECT em.*, emg.* FROM " . static::$table . " as em 
                LEFT JOIN event_market_groups as emg ON emg.event_market_id = em.id WHERE event_id = '{$eventId}'";
                echo $sql;
        return $connection->query($sql);
    }
}