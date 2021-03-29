<?php

namespace Models;

use Models\Model;

class MasterLeague extends Model
{
    protected static $table = 'master_leagues';

    public static function getSideBarLeaguesBySportAndGameSchedule($connection, int $sportId, int $primaryProviderId, int $maxMissingCount, string $gameSchedule)
    {
        $sql = "SELECT name, COUNT(name) AS match_count
            FROM (
                SELECT COALESCE(ml.name, l.name) as name
                FROM " . static::$table . " as ml
                JOIN league_groups as lg
                    ON lg.master_league_id = ml.id
                JOIN leagues as l
                    ON lg.league_id = l.id
                JOIN master_events as me
                    ON me.master_league_id = ml.id
                JOIN event_groups as eg
                    ON eg.master_event_id = me.id
                WHERE EXISTS (
                    SELECT 1
                    FROM events as e
                    WHERE e.id = eg.event_id
                        AND e.deleted_at is null
                        AND game_schedule = '{$gameSchedule}'
                        AND provider_id = {$primaryProviderId}
                        AND missing_count <= {$maxMissingCount}
                )
                AND provider_id = {$primaryProviderId}
                AND l.sport_id = {$sportId}
            ) as sidebar_leagues
            GROUP BY name
            ORDER BY name";

        return $connection->query($sql);
    }
}