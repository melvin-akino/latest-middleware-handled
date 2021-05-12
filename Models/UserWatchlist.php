<?php

namespace Models;

use Models\Model;

class UserWatchlist extends Model
{
    protected static $table = 'user_watchlist';

    public static function getUserWatchlists($connection, $providerId, $schedule)
    {
        $masterEventIds = [];
        $sql = "SELECT master_event_id FROM " . self::$table 
        ." JOIN event_groups on event_groups.master_event_id=".self::$table.".master_event_id" 
        ." JOIN events on events.id=event_groups.event_id"
        ." WHERE provider_id={$providerId} AND events.deleted_at is null AND game_schedule='{$schedule}'";
        $userWatchlists = $connection->query($sql);
        
        if ($userWatchlists) {
            $masterEventIdArray = $connection->fetchAll($userWatchlists);
            foreach($masterEventIdArray as $event) {
                $masterEventIds[] = $event['master_event_id'];
            }
        }
        return $masterEventIds;
    }
}