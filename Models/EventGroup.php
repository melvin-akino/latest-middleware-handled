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

    public static function getEventByMasterEventId($connection, $masterEventId)
    {
        $sql = "SELECT * FROM " . static::$table . " 
                JOIN events ON events.id = " . static::$table . ".event_id WHERE " . static::$table . ".master_event_id = '{$masterEventId}' AND deleted_at is null LIMIT 1";

        return $connection->query($sql);
    }

    public static function getDataByEventId($connection, $eventId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE event_id = '{$eventId}' LIMIT 1";
        return $connection->query($sql);
    }
}