<?php

namespace Models;

use Models\Model;

class EventMarketGroup extends Model
{
    protected static $table = 'event_market_groups';

    public static function checkIfMatched($connection, $eventMarketId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE event_market_id = '{$eventMarketId}'";
        return $connection->query($sql);
    }

    public static function getDataByEventMarketId($connection, $eventMarketId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE event_market_id = '{$eventMarketId}' LIMIT 1";
        return $connection->query($sql);
    }

    public static function deleteMatchesOfEventMarket($connection, $masterEventMarketId)
    {
        $sql = "DELETE FROM " . self::$table . " WHERE master_event_market_id = {$masterEventMarketId}";
        return $connection->query($sql);
    }
}