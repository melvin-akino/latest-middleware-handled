<?php

use Models\{
    Provider,
    ProviderAccount,
    SystemConfiguration,
    Order,
    Team,
    TeamGroup,
    League,
    LeagueGroup,
    Event,
    EventGroup,
    EventMarket,
    EventMarketGroup,
    Sport,
    SportOddType

};

// use App\Models\{
//     MasterEvent,
//     MasterLeague,
//     MasterTeam,
//     SportOddType,
//     Sport,
//     Provider,
//     SystemConfiguration
// };
// use Hhxsv5\LaravelS\Swoole\Process\CustomProcessInterface;
// use Swoole\{Http\Server, Coroutine, Process};
// use Illuminate\Support\Facades\{DB, Log};
// use App\Wrappers\SwooleStats;

class PreProcess
{
    /**
     * @var bool Quit tag for Reload updates
     */
    private static $connection;


    private static $quit = false;
    public static  $configTable;
    public static  $sportsTable;
    public static  $sportsOddTypesTable;
    public static  $providersTable;
    public static  $eventsTable;
    public static  $providerAccountsTable;
    public static  $eventsIndexTable;
    public static  $masterEventsTable;
    public static  $masterEventsIndexTable;
    public static  $leaguesTable;
    public static  $leaguesIndexTable;
    public static  $masterLeaguesTable;
    public static  $masterLeaguesIndexTable;
    public static  $teamsTable;
    public static  $teamsIndexTable;
    public static  $masterTeamsTable;
    public static  $masterTeamsIndexTable;
    public static  $eventMarketsTable;
    public static  $eventMarketsIndexTable;
    public static  $masterEventMarketsTable;
    public static  $masterEventMarketsIndexTable;
    public static  $eventScoresTable;
    public static  $eventMarketListTable;
    public static  $masterEventMarketListTable;
    public static  $eventsIndexKeysTable;
    public static  $masterEventsIndexKeysTable;
    public static  $ordersTable;
    public static  $maintenanceTable;

    public static function init($connection)
    {
        self::$connection = $connection;
    }

    // public static function init()
    // {
    //     self::$configTable                  = app('swoole')->configTable;
    //     self::$sportsTable                  = app('swoole')->enabledSportsTable;
    //     self::$sportsOddTypesTable          = app('swoole')->sportsOddTypesTable;
    //     self::$providersTable               = app('swoole')->enabledProvidersTable;
    //     self::$eventsTable                  = app('swoole')->eventsTable;
    //     self::$eventsIndexTable             = app('swoole')->eventsIndexTable;
    //     self::$masterEventsTable            = app('swoole')->masterEventsTable;
    //     self::$masterEventsIndexTable       = app('swoole')->masterEventsIndexTable;
    //     self::$leaguesTable                 = app('swoole')->leaguesTable;
    //     self::$leaguesIndexTable            = app('swoole')->leaguesIndexTable;
    //     self::$masterLeaguesTable           = app('swoole')->masterLeaguesTable;
    //     self::$masterLeaguesIndexTable      = app('swoole')->masterLeaguesIndexTable;
    //     self::$teamsTable                   = app('swoole')->teamsTable;
    //     self::$teamsIndexTable              = app('swoole')->teamsIndexTable;
    //     self::$masterTeamsTable             = app('swoole')->masterTeamsTable;
    //     self::$masterTeamsIndexTable        = app('swoole')->masterTeamsIndexTable;
    //     self::$eventMarketsTable            = app('swoole')->eventMarketsTable;
    //     self::$eventMarketsIndexTable       = app('swoole')->eventMarketsIndexTable;
    //     self::$masterEventMarketsTable      = app('swoole')->masterEventMarketsTable;
    //     self::$masterEventMarketsIndexTable = app('swoole')->masterEventMarketsIndexTable;
    //     self::$eventScoresTable             = app('swoole')->eventScoresTable;
    //     self::$eventMarketListTable         = app('swoole')->eventMarketListTable;
    //     self::$masterEventMarketListTable   = app('swoole')->masterEventMarketListTable;
    //     self::$eventsIndexKeysTable         = app('swoole')->eventsIndexKeysTable;
    //     self::$masterEventsIndexKeysTable   = app('swoole')->masterEventsIndexKeysTable;
    //     self::$providerAccountsTable        = app('swoole')->providerAccountsTable;
    //     self::$ordersTable                  = app('swoole')->activeOrdersTable;
    //     self::$maintenanceTable             = app('swoole')->maintenanceTable;

    //     self::$configTable["processKafka"]["value"] = 0;

    //     self::loadEnabledSports();
    //     self::loadSportsOddTypes();
    //     self::loadEnabledProviders();
    //     self::loadLeagues();
    //     self::loadMasterLeagues();
    //     self::loadTeams();
    //     self::loadMasterTeams();
    //     self::loadEvents();
    //     self::loadEnabledProviderAccounts();
    //     self::loadActiveOrders();
    //     self::loadMasterEvents();
    //     self::loadEventMarkets();
    //     self::loadMasterEventMarkets();
    //     self::loadEventScores();
    //     self::loadMaintenance();
    //     self::processSHM();

    //     Log::channel("ML_DB")->info("********************* [ ML-DB Starting up ] ****************************");
    //     self::$configTable["processKafka"]["value"] = 1;
    // }

    public static function loadEnabledSports()
    {
        global $swooleTable;

        foreach ($swooleTable['enabledSports'] as $k => $st) {
            $swooleTable['enabledSports']->del($k);
        }

        $result = Sport::getActiveSports(self::$connection);
        while ($sport = self::$connection->fetchAssoc($result)) {
            $swooleTable['enabledSports'][$sport['id']]["value"] = $sport['slug'];
        }
    }

    public static function loadSportsOddTypes()
    {
        global $swooleTable;

        foreach ($swooleTable['sportsOddTypes'] as $k => $st) {
            $swooleTable['sportsOddTypes']->del($k);
        }

        $result = SportOddType::getOddTypes(self::$connection);
        while ($sot = self::$connection->fetchAssoc($result)) {
            $swooleTable['sportsOddTypes'][$sot['sport_id'] . '-' . $sot['type']]["value"] = $sot['odd_type_id'];
        }
    }

    public static function loadEnabledProviders()
    {
        global $swooleTable;

        foreach ($swooleTable['enabledProviders'] as $k => $st) {
            $swooleTable['enabledProviders']->del($k);
        }

        $result = Provider::getActiveProviders(self::$connection);
        while ($p = self::$connection->fetchAssoc($result)) {
            $swooleTable['enabledProviders'][strtolower($p["alias"])]["value"]      = $p["id"];
            $swooleTable['enabledProviders'][strtolower($p["alias"])]["currencyId"] = $p["currency_id"];
        }
    }

    // private static function loadLeagues()
    // {
    //     foreach (self::$leaguesTable as $k => $l) {
    //         self::$leaguesTable->del($k);
    //     }

    //     foreach (self::$leaguesIndexTable as $k => $l) {
    //         self::$leaguesIndexTable->del($k);
    //     }

    //     $leagues = DB::table('leagues as l')
    //                  ->leftJoin('master_leagues as ml', 'ml.id', 'l.master_league_id')
    //                  ->whereNull('l.deleted_at')
    //                  ->whereNUll('ml.deleted_at')
    //                  ->select('l.*')
    //                  ->get()
    //                  ->toArray();

    //     foreach ($leagues as $league) {
    //         self::$leaguesTable->set($league->id,
    //             [
    //                 'sport_id'    => $league->sport_id,
    //                 'provider_id' => $league->provider_id,
    //                 'name'        => $league->name
    //             ]
    //         );
    //         self::$leaguesIndexTable->set(md5(implode(':', [$league->name, $league->sport_id, $league->provider_id])),
    //             [
    //                 'league_id' => $league->id
    //             ]
    //         );
    //     }
    // }

    // public static function loadMasterLeagues()
    // {
    //     global $swooleTable;
        
    //     foreach ($swooleTable['masterLeagues'] as $k => $ml) {
    //         $swooleTable['masterLeagues']->del($k);
    //     }

    //     foreach ($swooleTable['masterLeaguesIndex'] as $k => $ml) {
    //         $swooleTable['masterLeaguesIndex']->del($k);
    //     }

    //     // $masterLeagues = MasterLeague::all()->toArray();
    //     $result = MasterLeague::getAll(self::$connection);
    //     while ($masterLeague = self::$connection->fetchAssoc($result)) {
    //         $swooleTable['masterLeagues']->set($masterLeague['id'],
    //             [
    //                 'sport_id' => $masterLeague['sport_id'],
    //                 'name'     => $masterLeague['name']
    //             ]
    //         );

    //         $swooleTable['masterLeaguesIndex']->set(md5(implode(':', [$masterLeague['name'], $masterLeague['sport_id']])),
    //             [
    //                 'master_league_id' => $masterLeague['id']
    //             ]
    //         );
    //     }
    // }

    public static function loadLeagues()
    {
        global $swooleTable;

        foreach ($swooleTable['leagues'] as $k => $ml) {
            $swooleTable['leagues']->del($k);
        }

        $result = League::getActiveLeagues(self::$connection);
        while ($league = self::$connection->fetchAssoc($result)) {
            $swooleTable['leagues']->set(md5(implode(':', [$league['sport_id'], $league['provider_id'], $league['name']])),
                [
                    'id'          => $league['id'],
                    'name'        => $league['name'],
                    'sport_id'    => $league['sport_id'],
                    'provider_id' => $league['provider_id'],
                ]
            );
        }
    }

    public static function loadLeagueGroups()
    {
        global $swooleTable;
        
        foreach ($swooleTable['leagueGroups'] as $k => $lg) {
            $swooleTable['leagueGroups']->del($k);
        }

        $result = LeagueGroup::getLeaguesData(self::$connection);
        while ($league = self::$connection->fetchAssoc($result)) {
            $swooleTable['leagueGroups']->set(md5(implode(':', [$league['sport_id'], $league['provider_id'], $league['name']])),
                [
                    'league_id'        => $league['league_id'],
                    'master_league_id' => $league['master_league_id']
                ]
            );
        }
    }

    public static function loadTeams()
    {
        global $swooleTable;

        foreach ($swooleTable['teams'] as $k => $ml) {
            $swooleTable['teams']->del($k);
        }

        $result = Team::getActiveTeams(self::$connection);
        while ($team = self::$connection->fetchAssoc($result)) {
            $swooleTable['teams']->set(md5(implode(':', [$league['sport_id'], $league['provider_id'], $league['name']])),
                [
                    'id'          => $team['id'],
                    'name'        => $team['name'],
                    'sport_id'    => $team['sport_id'],
                    'provider_id' => $team['provider_id'],
                ]
            );
        }
    }

    public static function loadTeamGroups()
    {
        global $swooleTable;
        
        foreach ($swooleTable['teamGroups'] as $k => $lg) {
            $swooleTable['teamGroups']->del($k);
        }

        $result = teamGroup::getTeamsData(self::$connection);
        while ($team = self::$connection->fetchAssoc($result)) {
            $swooleTable['teamGroups']->set(md5(implode(':', [$league['sport_id'], $league['provider_id'], $league['name']])),
                [
                    'team_id'        => $league['team_id'],
                    'master_team_id' => $league['master_team_id']
                ]
            );
        }
    }

    public static function loadEvents()
    {
        global $swooleTable;

        foreach ($swooleTable['events'] as $k => $ml) {
            $swooleTable['events']->del($k);
        }

        $result = Event::getActiveEvents(self::$connection);
        while ($event = self::$connection->fetchAssoc($result)) {
            $swooleTable['events']->set(md5(implode(':', [$event['sport_id'], $event['provider_id'], $event['event_identifier']])),
                [
                    'id'            => $event['id'],
                    'sport_id'      => $event['sport_id'],
                    'provider_id'   => $event['provider_id'],
                    'missingCount'  => $event['missing_count'],
                    'league_id'     => $event['league_id'],
                    'team_home_id'  => $event['team_home_id'],
                    'team_away_id'  => $event['team_away_id'],
                    'ref_schedule'  => $event['ref_schedule'],
                    'game_schedule' => $event['game_schedule']
                ]
            );
        }
    }

    public static function loadEventGroups()
    {
        global $swooleTable;
        
        foreach ($swooleTable['eventGroups'] as $k => $lg) {
            $swooleTable['eventGroups']->del($k);
        }

        $result = EventGroup::getEventsData(self::$connection);
        while ($event = self::$connection->fetchAssoc($result)) {
            $swooleTable['eventGroups']->set(md5(implode(':', [$event['sport_id'], $event['provider_id'], $event['event_identifier']])),
                [
                    'event_id'        => $event['event_id'],
                    'master_event_id' => $event['master_event_id']
                ]
            );
        }
    }

    public static function loadEventMarkets()
    {
        global $swooleTable;

        foreach ($swooleTable['eventMarkets'] as $k => $ml) {
            $swooleTable['eventMarkets']->del($k);
        }

        $result = EventMarket::getActiveEventMarkets(self::$connection);
        while ($eventMarket = self::$connection->fetchAssoc($result)) {
            $swooleTable['eventMarkets']->set(md5(implode(':', [$eventMarket['provider_id'], $eventMarket['bet_identifier']])),
                [
                    'id'            => $eventMarket['id'],
                    'bet_identifier'   => $eventMarket['bet_identifier'],
                    'event_id'  => $eventMarket['event_id'],
                    'provider_id'     => $eventMarket['provider_id'],
                    'odd_type_id'  => $eventMarket['odd_type_id'],
                    'market_event_identifier'  => $eventMarket['market_event_identifier'],
                    'market_flag'  => $eventMarket['market_flag'],
                    'is_main' => $eventMarket['is_main'],
                    'odd_label'  => $eventMarket['odd_label'],
                    'odds'  => $eventMarket['odds']
                ]
            );
        }
    }

    public static function loadEventMarketGroups()
    {
        global $swooleTable;
        
        foreach ($swooleTable['eventMarketGroups'] as $k => $lg) {
            $swooleTable['eventMarketGroups']->del($k);
        }

        $result = EventMarketGroup::getEventMarketsData(self::$connection);
        while ($eventMarket = self::$connection->fetchAssoc($result)) {
            $swooleTable['eventMarketGroups']->set(md5(implode(':', [$eventMarket['provider_id'], $eventMarket['bet_identifier']])),
                [
                    'event_market_id'        => $eventMarket['event_market_id'],
                    'master_event_market_id' => $eventMarket['master_event_market_id']
                ]
            );
        }
    }

    // private static function loadTeams()
    // {
    //     foreach (self::$teamsTable as $k => $t) {
    //         self::$teamsTable->del($k);
    //     }

    //     foreach (self::$teamsIndexTable as $k => $t) {
    //         self::$teamsIndexTable->del($k);
    //     }

    //     $teams = DB::table('teams as t')
    //                ->leftJoin('master_teams as mt', 'mt.id', 't.id')
    //                ->whereNull('t.deleted_at')
    //                ->whereNull('mt.deleted_at')
    //                ->select('t.*')
    //                ->get()
    //                ->toArray();

    //     foreach ($teams as $team) {
    //         self::$teamsTable->set($team->id,
    //             [
    //                 'sport_id'    => $team->sport_id,
    //                 'provider_id' => $team->provider_id,
    //                 'name'        => $team->name
    //             ]
    //         );

    //         self::$teamsIndexTable->set(md5(implode(':', [$team->name, $team->sport_id, $team->provider_id])),
    //             [
    //                 'team_id' => $team->id
    //             ]
    //         );
    //     }
    // }

    // private static function loadMasterTeams()
    // {
    //     foreach (self::$masterTeamsTable as $k => $mt) {
    //         self::$masterTeamsTable->del($k);
    //     }

    //     foreach (self::$masterTeamsIndexTable as $k => $mt) {
    //         self::$masterTeamsIndexTable->del($k);
    //     }

    //     $masterTeams = MasterTeam::all()->toArray();
    //     foreach ($masterTeams as $masterTeam) {
    //         self::$masterTeamsTable->set($masterTeam['id'],
    //             [
    //                 'sport_id' => $masterTeam['sport_id'],
    //                 'name'     => $masterTeam['name']
    //             ]
    //         );

    //         self::$masterTeamsIndexTable->set(md5(implode(':', [$masterTeam['name'], $masterTeam['sport_id']])),
    //             [
    //                 'master_team_id' => $masterTeam['id']
    //             ]
    //         );
    //     }
    // }

    // private static function loadEvents()
    // {
    //     foreach (self::$eventsTable as $k => $e) {
    //         self::$eventsTable->del($k);
    //     }

    //     foreach (self::$eventsIndexTable as $k => $e) {
    //         self::$eventsIndexTable->del($k);
    //     }

    //     $myEvents = DB::table('events AS e')
    //                   ->leftJoin('master_events AS me', 'e.master_event_id', '=', 'me.id')
    //                   ->leftJoin('providers as p', 'e.provider_id', '=', 'p.id')
    //                   ->whereNull('me.deleted_at')
    //                   ->whereNull('e.deleted_at')
    //                   ->select('e.*', 'p.alias', 'me.game_schedule')
    //                   ->get()
    //                   ->toArray();

    //     foreach ($myEvents as $e) {
    //         if (self::$providersTable->exists(strtolower($e->alias))) {
    //             self::$eventsTable->set($e->event_identifier, [
    //                 "sport"           => $e->sport_id,
    //                 "schedule"        => $e->game_schedule,
    //                 "provider"        => $e->provider_id,
    //                 "missingCount"    => $e->missing_count,
    //                 "id"              => $e->id,
    //                 'league_id'       => $e->league_id,
    //                 'team_home_id'    => $e->team_home_id,
    //                 'team_away_id'    => $e->team_away_id,
    //                 'master_event_id' => $e->master_event_id,
    //                 'ref_schedule'    => $e->ref_schedule
    //             ]);

    //             self::$eventsIndexTable->set(md5(implode(':', [$e->league_id, $e->team_home_id, $e->team_away_id, $e->master_event_id, $e->ref_schedule])), [
    //                 "event_identifier" => $e->event_identifier
    //             ]);

    //             self::$eventsIndexKeysTable->set($e->event_identifier, [
    //                 "eventIndexHash" => md5(implode(':', [$e->league_id, $e->team_home_id, $e->team_away_id, $e->master_event_id, $e->ref_schedule]))
    //             ]);
    //         } else {
    //             Log::channel('ML_DB')->warning("LoadEvents: Event for non-active Provider: " . var_export($e, true));
    //         }
    //     }
    // }

    // private static function loadMasterEvents()
    // {
    //     foreach (self::$masterEventsTable as $k => $me) {
    //         self::$masterEventsTable->del($k);
    //     }

    //     foreach (self::$masterEventsIndexTable as $k => $me) {
    //         self::$masterEventsIndexTable->del($k);
    //     }

    //     $masterEvents = MasterEvent::all()->toArray();
    //     foreach ($masterEvents as $masterEvent) {
    //         self::$masterEventsTable->set($masterEvent['id'],
    //             [
    //                 'master_event_unique_id' => $masterEvent['master_event_unique_id'],
    //                 'sport_id'               => $masterEvent['sport_id'],
    //                 'ref_schedule'           => $masterEvent['ref_schedule'],
    //                 'master_league_id'       => $masterEvent['master_league_id'],
    //                 'master_team_home_id'    => $masterEvent['master_team_home_id'],
    //                 'master_team_away_id'    => $masterEvent['master_team_away_id'],
    //                 'game_schedule'          => $masterEvent['game_schedule'],
    //                 'score'                  => $masterEvent['score']
    //             ]
    //         );

    //         self::$masterEventsIndexTable->set($masterEvent['master_event_unique_id'],
    //             [
    //                 'master_event_id' => $masterEvent['id']
    //             ]
    //         );

    //         $masterEventHash = md5(implode(':', [
    //             $masterEvent['master_league_id'],
    //             $masterEvent['master_team_home_id'],
    //             $masterEvent['master_team_away_id'],
    //             $masterEvent['sport_id'],
    //             date('Ymd', strtotime($masterEvent['ref_schedule']))
    //         ]));

    //         self::$masterEventsIndexTable->set($masterEventHash,
    //             [
    //                 'master_event_id' => $masterEvent['id']
    //             ]
    //         );

    //         self::$masterEventsIndexTable->set($masterEvent['id'],
    //             [
    //                 'masterEventIndexHash' => $masterEventHash
    //             ]
    //         );

    //         self::$masterEventsIndexTable->set($masterEventHash,
    //             [
    //                 'masterEventIndexHash' => $masterEvent['master_event_unique_id']
    //             ]
    //         );
    //     }
    // }

    // private static function loadEventMarkets()
    // {
    //     foreach (self::$eventMarketsTable as $k => $em) {
    //         self::$eventMarketsTable->del($k);
    //     }

    //     foreach (self::$eventMarketsIndexTable as $k => $em) {
    //         self::$eventMarketsIndexTable->del($k);
    //     }

    //     foreach (self::$eventMarketListTable as $k => $em) {
    //         self::$eventMarketListTable->del($k);
    //     }

    //     $eventMarkets = DB::table('event_markets as em')
    //                       ->leftJoin('master_event_markets as mem', 'mem.id', 'em.master_event_market_id')
    //                       ->whereNull('em.deleted_at')
    //                       ->select('em.*')
    //                       ->get()
    //                       ->toArray();

    //     $activeMarkets = [];
    //     foreach ($eventMarkets as $eventMarket) {
    //         $activeMarkets[] = $eventMarket->bet_identifier;
    //         self::$eventMarketsTable->set($eventMarket->bet_identifier,
    //             [
    //                 'master_event_market_id'  => $eventMarket->master_event_market_id,
    //                 'bet_identifier'          => $eventMarket->bet_identifier,
    //                 'event_id'                => $eventMarket->event_id,
    //                 'provider_id'             => $eventMarket->provider_id,
    //                 'odd_type_id'             => $eventMarket->odd_type_id,
    //                 'market_event_identifier' => $eventMarket->market_event_identifier,
    //                 'market_flag'             => $eventMarket->market_flag,
    //                 'is_main'                 => $eventMarket->is_main,
    //                 'odd_label'               => $eventMarket->odd_label,
    //                 'odds'                    => $eventMarket->odds
    //             ]
    //         );

    //         self::$eventMarketsIndexTable->set(implode(':', [$eventMarket->event_id, $eventMarket->master_event_market_id]),
    //             [
    //                 'bet_identifier' => $eventMarket->bet_identifier
    //             ]
    //         );

    //         $activeMarkets = self::$eventMarketListTable->get($eventMarket->event_id, 'marketIDs');
    //         if ($activeMarkets) {
    //             $activeMarkets   = json_decode($activeMarkets, true);
    //             $activeMarkets[] = $eventMarket->bet_identifier;
    //         } else {
    //             $activeMarkets = [];
    //         }
    //         self::$eventMarketListTable->set($eventMarket->event_id, ['marketIDs' => json_encode($activeMarkets)]);
    //     }
    // }

    // private static function loadMasterEventMarkets()
    // {
    //     foreach (self::$masterEventMarketsTable as $k => $mem) {
    //         self::$masterEventMarketsTable->del($k);
    //     }

    //     foreach (self::$masterEventMarketsIndexTable as $k => $mem) {
    //         self::$masterEventMarketsIndexTable->del($k);
    //     }

    //     $masterEventMarkets = DB::table('master_event_markets as mem')
    //                             ->join('event_markets as em', 'mem.id', 'em.master_event_market_id')
    //                             ->whereNull('em.deleted_at')
    //                             ->select('mem.id', 'mem.is_main', 'mem.master_event_id', 'mem.master_event_market_unique_id')
    //                             ->get()
    //                             ->toArray();

    //     foreach ($masterEventMarkets as $masterEventMarket) {
    //         self::$masterEventMarketsTable->set($masterEventMarket->id,
    //             [
    //                 'is_main'                       => $masterEventMarket->is_main,
    //                 'master_event_id'               => $masterEventMarket->master_event_id,
    //                 'master_event_market_unique_id' => $masterEventMarket->master_event_market_unique_id,
    //             ]
    //         );

    //         self::$masterEventMarketsIndexTable->set($masterEventMarket->master_event_market_unique_id,
    //             [
    //                 'master_event_market_id' => $masterEventMarket->id
    //             ]
    //         );

    //         $activeMarkets = self::$masterEventMarketListTable->get($masterEventMarket->master_event_id, 'memUIDs');
    //         if ($activeMarkets) {
    //             $activeMarkets   = json_decode($activeMarkets, true);
    //             $activeMarkets[] = $masterEventMarket->master_event_market_unique_id;
    //         } else {
    //             $activeMarkets = [];
    //         }
    //         self::$masterEventMarketListTable->set($masterEventMarket->master_event_id, ['memUIDs' => json_encode($activeMarkets)]);
    //     }
    // }

    // private static function loadEventScores()
    // {
    //     foreach (self::$eventScoresTable as $k => $mt) {
    //         self::$eventScoresTable->del($k);
    //     }

    //     $eventScores = DB::table('event_scores as es')
    //                      ->join('master_events as me', 'me.master_event_unique_id', 'es.master_event_unique_id')
    //                      ->join('events as e', 'e.master_event_id', 'me.id')
    //                      ->whereNull('me.deleted_at')
    //                      ->whereNull('e.deleted_at')
    //                      ->select('es.*', 'e.event_identifier')
    //                      ->get()
    //                      ->toArray();
    //     foreach ($eventScores as $eventScore) {
    //         self::$eventScoresTable[$eventScore->event_identifier]['value'] = 1;
    //     }
    // }

    // private static function processSHM()
    // {
    //     self::$configTable["processKafka"]["value"] = 0;
    //     processTaskTempDir(true);
    //     self::$configTable["processKafka"]["value"] = 1;
    // }

    public static function loadEnabledProviderAccounts()
    {
        global $swooleTable;

        foreach ($swooleTable['providerAccounts'] as $k => $pa) {
            $swooleTable['providerAccounts']->del($k);
        }

        $result = ProviderAccount::getEnabledProviderAccounts(self::$connection);

        while ($providerAccount = self::$connection->fetchAssoc($result)) {
            $swooleTable['providerAccounts'][$providerAccount['id']] = [
                'provider_id'       => $providerAccount['provider_id'],
                'username'          => $providerAccount['username'],
                'punter_percentage' => $providerAccount['punter_percentage'],
                'credits'           => $providerAccount['credits'],
                'alias'             => $providerAccount['alias'],
                'type'              => $providerAccount['type']
            ];
        }

        // foreach ($providerAccounts as $providerAccount) {
        //     self::$providerAccountsTable->set($providerAccount->id,
        //         [
        //             'provider_id'       => $providerAccount->provider_id,
        //             'username'          => $providerAccount->username,
        //             'punter_percentage' => $providerAccount->punter_percentage,
        //             'credits'           => $providerAccount->credits,
        //             'alias'             => $providerAccount->alias,
        //             'type'              => $providerAccount->type
        //         ]
        //     );
        // }
    }

    public static function loadActiveOrders()
    {
        global $swooleTable;

        foreach ($swooleTable['activeOrders'] as $k => $e) {
            $swooleTable['activeOrders']->del($k);
        }

        $result = Order::getActiveOrders(self::$connection);
        $orders = self::$connection->fetchAll($result);

        foreach ($orders as $order) {
            $swooleTable['activeOrders']->set($order['id'], [
                'createdAt'      => $order['created_at'],
                'betId'          => $order['bet_id'],
                'orderExpiry'    => $order['order_expiry'],
                'username'       => $order['username'],
                'userCurrencyId' => $order['user_currency_id'],
                'status'         => $order['status']
            ]);
        }
    }

    public static function loadMaintenance()
    {
        global $swooleTable;

        foreach ($swooleTable['maintenance'] as $k => $m) {
            $swooleTable['maintenance']->del($k);
        }

        $result = SystemConfiguration::getProviderMaintenanceConfigData(self::$connection);

        while ($maintenance = self::$connection->fetchAssoc($result)) {
            $maintenanceTypes = explode('_', $maintenance['type']);
            $provider         = strtolower($maintenanceTypes[0]);

            if ($swooleTable['enabledProviders']->exists($provider)) {
                $swooleTable['enabledProviders']->set($provider, ['under_maintenance' => $maintenance->value]);
            }
        }
    }

    // public static function callback(Server $swoole, Process $process)
    // {
    //     self::init();

    //     $reportTime = 0;
    //     $reloadTime = 0;
    //     while (!self::$quit) {
    //         if ($reportTime == 10) {
    //             Log::channel('ML_DB')->info("Calling Report Stats");
    //             $reportTime = 0;
    //             SwooleStats::reportStats('events');
    //             SwooleStats::reportStats('odds');
    //         }

    //         if ($reloadTime == 300) {
    //             Log::channel('ML_DB')->info("Loading Active Orders For New Records");
    //             self::loadActiveOrders();
    //             $reloadTime = 0;
    //         }

    //         $reportTime++;
    //         $reloadTime++;
    //         Coroutine::sleep(1); // Swoole>=2.1: Coroutine & Runtime will be automatically enabled for callback().
    //     }
    // }
}

?>
