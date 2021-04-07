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

    public static function getEventsByMasterEventId($connection, $masterEventId)
    {
        $sql = "SELECT * FROM " . static::$table . " 
                JOIN events ON events.id = " . static::$table . ".event_id WHERE " . static::$table . ".master_event_id = '{$masterEventId}' AND deleted_at is null";

        return $connection->query($sql);
    }

    public static function getDataByEventId($connection, $eventId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE event_id = '{$eventId}' LIMIT 1";
        return $connection->query($sql);
    }

    public static function getMatchedEvents($connection, $masterEventId, $eventId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE master_event_id = {$masterEventId} AND event_id != {$eventId}";
        return $connection->query($sql);
    }

    public static function deleteMatchesOfEvent($connection, $masterEventId, $eventId)
    {
        $sql = "DELETE FROM " . self::$table . " WHERE master_event_id = {$masterEventId} AND event_id != {$eventId}";
        return $connection->query($sql);
    }

    public static function getAllActive($connection)
    {
        $sql = "SELECT eg.*, e.provider_id, e.sport_id FROM " . self::$table . " as eg
                JOIN events as e ON e.id = eg.event_id
                WHERE e.deleted_at is null";
        return $connection->query($sql);
    }
}