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
}