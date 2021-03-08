<?php

namespace Models;

use Models\Model;

class Event extends Model
{
    protected static $table = 'events';

    public static function getActiveEvents($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }

    public static function getEventByProviderParam($connection, $eventIdentifier, $providerId, $sportId)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE event_identifier = '{$eventIdentifier}' AND provider_id = '{$providerId}' AND sport_id = '{$sportId}' ORDER BY id DESC LIMIT 1";
        return $connection->query($sql);
    }
}