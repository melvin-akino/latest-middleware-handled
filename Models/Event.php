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

    public static function getAllUnmatchedEvents($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE id NOT IN (SELECT event_id FROM event_groups) ORDER BY id DESC";
        return $connection->query($sql);
    }

    public static function getAllGroupVerifiedUnmatchedEvents($connection)
    {
        $sql = "SELECT e.id as event_id, e.ref_schedule, lg.master_league_id, ht.master_team_id as master_home_team_id, at.master_team_id as master_away_team_id FROM " . static::$table . " as e"
        . " JOIN unmatched_data as ue on ue.data_id = e.id"
        . " JOIN team_groups as ht on ht.team_id = e.team_home_id"
        . " JOIN team_groups as at on at.away_team_id = e.team_away_id"
        . " JOIN league_groups as lg on lg.league_id = e.league_id"          
        . " WHERE ue.data_type = 'event'";
        return $connection->query($sql);
    }

    public static function getEventsById($connection, $eventId)
    {
        $sql = "SELECT e.*, eg.* FROM " . static::$table . " as e 
                LEFT JOIN event_groups as eg ON eg.event_id = e.id WHERE id = '{$eventId}'";
        return $connection->query($sql);
    }
}