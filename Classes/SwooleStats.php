<?php

use Illuminate\Support\Facades\Log;

class SwooleStats
{
    const NO_ERROR        = 1;
    const ERROR           = 2;
    const TIMESTAMP_ERROR = 3;
    const HASH_ERROR      = 4;

    const EVENTS_INACTIVE_SPORT    = 10;
    const EVENTS_INACTIVE_PROVIDER = 11;

    public static function getInstance()
    {
        return self;
    }

    public static function addStat($what)
    {
        $type = $what["type"];

        if ($type == "events") {
            $statsTable = app('swoole')->statsCountEventsPerSecondTable;
            $timeTable  = app('swoole')->statsTimeEventsPerSecondTable;
            $now        = time();
        } else if ($type == "odds") {
            $statsTable = app('swoole')->statsCountOddsPerSecondTable;
            $timeTable  = app('swoole')->statsTimeOddsPerSecondTable;
            $now        = time();
        } else {
            //
        }

        if (in_array($type, ['odds', 'events'])) {
            if (!$statsTable->exists($now)) {
                $statsTable->set($now, [
                    "total"            => 0,
                    "processed"        => 0,
                    "error"            => 0,
                    "inactiveSport"    => 0,
                    "inactiveProvider" => 0,
                ]);
            }
            if (!$timeTable->exists($now)) {
                $timeTable->set($now, [
                    "total"            => 0,
                    "processed"        => 0,
                    "error"            => 0,
                    "inactiveSport"    => 0,
                    "inactiveProvider" => 0,
                ]);
            }


            $totalCount               = $statsTable[$now]["total"];
            $avgTime                  = $timeTable[$now]["total"];
            $allTime                  = ($avgTime * $totalCount) + $what["time"];
            $timeTable[$now]["total"] = $allTime / ($totalCount + 1);
            $statsTable->incr($now, "total", 1);
            switch ($what["status"]) {
                case self::NO_ERROR:
                    $avgTime                      = $timeTable[$now]["processed"];
                    $allTime                      = ($avgTime * $totalCount) + $what["time"];
                    $timeTable[$now]["processed"] = $allTime / ($totalCount + 1);
                    $statsTable->incr($now, "processed", 1);
                    break;
                case self::ERROR:
                case self::TIMESTAMP_ERROR:
                case self::HASH_ERROR:
                    $avgTime                  = $timeTable[$now]["error"];
                    $allTime                  = ($avgTime * $totalCount) + $what["time"];
                    $timeTable[$now]["error"] = $allTime / ($totalCount + 1);
                    $statsTable->incr($now, "error", 1);
                    break;
                case self::EVENTS_INACTIVE_SPORT:
                    $avgTime                          = $timeTable[$now]["inactiveSport"];
                    $allTime                          = ($avgTime * $totalCount) + $what["time"];
                    $timeTable[$now]["inactiveSport"] = $allTime / ($totalCount + 1);
                    $statsTable->incr($now, "inactiveSport", 1);
                    break;
                case self::EVENTS_INACTIVE_PROVIDER:
                    $avgTime                             = $timeTable[$now]["inactiveProvider"];
                    $allTime                             = ($avgTime * $totalCount) + $what["time"];
                    $timeTable[$now]["inactiveProvider"] = $allTime / ($totalCount + 1);
                    $statsTable->incr($now, "inactiveProvider", 1);
                    break;
                default:
                    Log::warning("Odds stats got a status that was NOT correct!");
                    Log::warning(var_export($what, true));
                    break;
            }
        }

        #Log::channel("events")->info(var_export($myStats,true));
        #Log::channel("events")->info(var_export($statsTs,true));
    }


    public static function reportStats($which)
    {
        if ($which == "events") {
            Log::channel($which)->info("**************** start swoole table dump ****************");
            foreach (app('swoole')->statsCountEventsPerSecondTable as $k => $s) {
                if ($k > (time() - 60)) {
                    Log::channel($which)->info($k);
                    Log::channel($which)->info(var_export($s, true));
                    Log::channel($which)->info(var_export(app('swoole')->statsTimeEventsPerSecondTable[$k], true));
                } else {
                    app('swoole')->statsCountEventsPerSecondTable->del($k);
                    app('swoole')->statsTimeEventsPerSecondTable->del($k);
                }
            }
            Log::channel($which)->info("**************** end swoole table dump ****************");
        } else if ($which == 'odds') {
            Log::channel($which)->info("**************** start swoole table dump ****************");
            foreach (app('swoole')->statsCountOddsPerSecondTable as $k => $s) {
                if ($k > (time() - 60)) {
                    Log::channel($which)->info($k);
                    Log::channel($which)->info(var_export($s, true));
                    Log::channel($which)->info(var_export(app('swoole')->statsTimeOddsPerSecondTable[$k], true));
                } else {
                    app('swoole')->statsCountOddsPerSecondTable->del($k);
                    app('swoole')->statsTimeOddsPerSecondTable->del($k);
                }
            }
            Log::channel($which)->info([
                'leagues table count'                    => app('swoole')->leaguesTable->count(),
                'leagues index table count'              => app('swoole')->leaguesIndexTable->count(),
                'master leagues table count'             => app('swoole')->masterLeaguesTable->count(),
                'master index leagues table count'       => app('swoole')->masterLeaguesIndexTable->count(),
                'teams table count'                      => app('swoole')->teamsTable->count(),
                'teams index table count'                => app('swoole')->teamsIndexTable->count(),
                'master teams table count'               => app('swoole')->masterTeamsTable->count(),
                'master teams index table count'         => app('swoole')->masterTeamsIndexTable->count(),
                'events table count'                     => app('swoole')->eventsTable->count(),
                'events index table count'               => app('swoole')->eventsIndexTable->count(),
                'events index keys table count'          => app('swoole')->eventsIndexKeysTable->count(),
                'master events table count'              => app('swoole')->masterEventsTable->count(),
                'master events index table count'        => app('swoole')->masterEventsIndexTable->count(),
                'master events index keys table count'   => app('swoole')->masterEventsIndexKeysTable->count(),
                'event markets table count'              => app('swoole')->eventMarketsTable->count(),
                'event markets index table count'        => app('swoole')->eventMarketsIndexTable->count(),
                'event market list table count'          => app('swoole')->eventMarketListTable->count(),
                'master event markets table count'       => app('swoole')->masterEventMarketsTable->count(),
                'master event markets index table count' => app('swoole')->masterEventMarketsIndexTable->count(),
                'master event market list table count'   => app('swoole')->masterEventMarketListTable->count(),
            ]);
            Log::channel($which)->info("**************** end swoole table dump ****************");
        }
    }

    public static function getErrorType($type)
    {
        return constant('self::' . $type);
    }
}

?>
