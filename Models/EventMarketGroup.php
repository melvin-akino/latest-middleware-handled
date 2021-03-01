<?php

namespace Models;

use Models\Model;

Class EventMarketGroup extends Model
{
    protected static $table = 'event_market_groups';

    public static function getEventMarketsData($connection)
    {
        $sql = "SELECT emg.*, bet_identifier, provider_id  FROM " . self::$table . " as emg
                JOIN event_markets as em ON emg.event_market_id = em.id WHERE em.deleted_at is null";
        return $connection->query($sql);
    } 
}