<?php

namespace Models;

use Models\Model;

class EventGroup extends Model
{
    protected static $table = 'event_groups';

    public static function checkIfMatched($connection, $eventId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE event_id = '{$eventId}'";
        return $connection->query($sql);
    }
}