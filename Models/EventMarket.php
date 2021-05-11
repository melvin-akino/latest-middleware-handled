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
        echo $sql . "\n";
        return $connection->query($sql);
    }

    public static function getActiveEventMarkets($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }

    public static function getAllUnmatchedMarket($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE NOT EXISTS (SELECT null FROM event_market_groups as emg WHERE emg.event_market_id = event_markets.id) AND deleted_at is null ORDER BY id DESC";
        return $connection->query($sql);
    }

    public static function getAllUnmatchedMarketWithMatchedEvents($connection)
    {
        $sql = "SELECT em.*, eg.master_event_id FROM " . static::$table . " as em
            JOIN event_groups as eg ON eg.event_id = em.event_id
            WHERE NOT EXISTS (SELECT null FROM event_market_groups as emg WHERE emg.event_market_id = em.id) 
            AND em.deleted_at is null 
            ORDER BY id DESC";
        return $connection->query($sql);
    }

    public static function getMarketsByEventId($connection, $eventId)
    {
        $sql = "SELECT em.*, emg.* FROM " . static::$table . " as em 
                LEFT JOIN event_market_groups as emg ON emg.event_market_id = em.id WHERE event_id = '{$eventId}' AND em.deleted_at is null";
        return $connection->query($sql);
    }

    public static function getMarketsByMasterEventId($connection, $masterEventId)
    {
        $sql = "SELECT em.*, emg.* FROM " . static::$table . " as em 
                LEFT JOIN event_market_groups as emg ON emg.event_market_id = em.id
                JOIN event_groups as eg ON eg.event_id = em.event_id WHERE eg.master_event_id = '{$masterEventId}' AND em.deleted_at is null";
        return $connection->query($sql);
    }

    public static function getUnmatchedMarketByIds($connection, $eventMarketIds)
    {
        $whereIn = implode("','", $eventMarketIds);
        $sql = "SELECT em.* FROM " . static::$table . " as em 
                WHERE NOT EXISTS (SELECT null FROM event_market_groups as emg WHERE emg.event_market_id = em.id) AND em.id IN ('{$whereIn}')";

        return $connection->query($sql);
    }

    public static function getMarketsByMasterEventIds($connection, $masterEventIds)
    {
        $sql = "SELECT e.sport_id, e.game_schedule, em.*, emg.* FROM " . static::$table . " as em 
                LEFT JOIN event_market_groups as emg ON emg.event_market_id = em.id
                LEFT JOIN events as e on e.id=em.event_id
                JOIN event_groups as eg ON eg.event_id = em.event_id WHERE eg.master_event_id in ($masterEventIds) AND em.deleted_at is null";
        return $connection->query($sql);
    }
}