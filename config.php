<?php

$config = [
    'database' => [
        'connection_pool' => [
            'minActive'         => 10,
            'maxActive'         => 20,
            'maxWaitTime'       => 100,
            'maxIdleTime'       => 300,
            'idleCheckInterval' => 10,
        ],
        'pgsql' => [
            'host'        => getenv('DB_HOST', '127.0.0.1'),
            'port'        => getenv('DB_PORT', 5432),
            'dbname'      => getenv('DB_DATABASE', 'multilinev2'),
            'user'        => getenv('DB_USERNAME', 'postgres'),
            'password'    => getenv('DB_PASSWORD', 'password')
        ]
    ],
    'swoole_tables'            => [
        'config'                    => [
            'size'   => 2,
            'column' => [
                ['name' => 'value', 'type' => \Swoole\Table::TYPE_FLOAT],
            ],
        ],
        'timestamps'                => [
            'size'   => 2,
            'column' => [
                ['name' => 'ts', 'type' => \Swoole\Table::TYPE_FLOAT],
            ],
        ],
        'eventOddsHash'             => [
            'size'   => 2000,
            'column' => [
                ['name' => 'hash', 'type' => \Swoole\Table::TYPE_STRING, "size" => 37],
            ],
        ],
        'leagues'                   => [
            'size'   => 10000, //@TODO needs to be adjusted once additional provider comes in
            'column' => [
                ['name' => 'name', 'type' => \Swoole\Table::TYPE_STRING, "size" => 80],
                ['name' => 'sport_id', 'type' => \Swoole\Table::TYPE_INT],
                ['name' => 'provider_id', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'leaguesIndex'              => [
            'size'   => 10000, //@TODO needs to be adjusted once additional provider comes in
            'column' => [
                ['name' => 'league_id', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'masterLeagues'             => [
            'size'   => 10000,
            'column' => [
                ['name' => 'name', 'type' => \Swoole\Table::TYPE_STRING, "size" => 80],
                ['name' => 'sport_id', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'masterLeaguesIndex'        => [ // md5($leagueName . ':' . $sportId)
                                         'size'   => 10000,
                                         'column' => [
                                             ['name' => 'master_league_id', 'type' => \Swoole\Table::TYPE_INT],
                                         ],
        ],
        'teams'                     => [
            'size'   => 20000, //@TODO needs to be adjusted once additional provider comes in
            'column' => [
                ['name' => 'name', 'type' => \Swoole\Table::TYPE_STRING, "size" => 80],
                ['name' => 'sport_id', 'type' => \Swoole\Table::TYPE_INT],
                ['name' => 'provider_id', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'teamsIndex'                => [
            'size'   => 20000, //@TODO needs to be adjusted once additional provider comes in
            'column' => [
                ['name' => 'team_id', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'masterTeams'               => [
            'size'   => 20000,
            'column' => [
                ['name' => 'name', 'type' => \Swoole\Table::TYPE_STRING, "size" => 80],
                ['name' => 'sport_id', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'masterTeamsIndex'          => [
            'size'   => 20000,
            'column' => [
                ['name' => 'master_team_id', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'enabledSports'             => [
            'size'   => 100,
            'column' => [
                ['name' => 'value', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        'enabledProviders'          => [
            'size'   => 100,
            'column' => [
                ['name' => 'value', 'type' => \Swoole\Table::TYPE_FLOAT],
                ['name' => 'currencyId', 'type' => \Swoole\Table::TYPE_INT],
            ],
        ],
        "events"                    => [
            "size"   => 10000,
            "column" => [
                ["name" => "sport", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "schedule", "type" => \Swoole\Table::TYPE_STRING, "size" => 6],
                ["name" => "provider", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "missingCount", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "league_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "team_home_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "team_away_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "master_event_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "ref_schedule", "type" => \Swoole\Table::TYPE_STRING, "size" => 20]
            ],
        ],
        "eventsIndex"               => [
            "size"   => 10000,
            "column" => [
                ["name" => "event_identifier", "type" => \Swoole\Table::TYPE_STRING, "size" => 30]
            ],
        ],
        "eventsIndexKeys"           => [
            "size"   => 10000,
            "column" => [
                ["name" => "eventIndexHash", "type" => \Swoole\Table::TYPE_STRING, "size" => 32]
            ],
        ],
        "masterEvents"              => [
            "size"   => 10000,
            "column" => [
                ["name" => "master_event_unique_id", "type" => \Swoole\Table::TYPE_STRING, "size" => 30],
                ["name" => "sport_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "ref_schedule", "type" => \Swoole\Table::TYPE_STRING, "size" => 20],
                ["name" => "master_league_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "master_team_home_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "master_team_away_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "game_schedule", "type" => \Swoole\Table::TYPE_STRING, "size" => 6],
                ["name" => "score", "type" => \Swoole\Table::TYPE_STRING, "size" => 7],
                ["name" => "running_time", "type" => \Swoole\Table::TYPE_STRING, "size" => 15],
                ["name" => "home_penalty", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "home_penalty", "type" => \Swoole\Table::TYPE_INT],
            ],
        ],
        "masterEventsIndex"         => [
            "size"   => 10000,
            "column" => [
                ["name" => "master_event_id", "type" => \Swoole\Table::TYPE_INT]
            ],
        ],
        "masterEventsIndexKeys"     => [
            "size"   => 10000,
            "column" => [
                ["name" => "masterEventIndexHash", "type" => \Swoole\Table::TYPE_STRING, "size" => 32]
            ],
        ],
        "eventScores"               => [
            "size"   => 10000,
            "column" => [
                ["name" => "value", "type" => \Swoole\Table::TYPE_INT]
            ],
        ],
        "eventMarkets"              => [
            "size"   => 100000,
            "column" => [
                ["name" => "master_event_market_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "bet_identifier", "type" => \Swoole\Table::TYPE_STRING, "size" => 20],
                ["name" => "event_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "provider_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "odd_type_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "market_event_identifier", "type" => \Swoole\Table::TYPE_STRING, "size" => 30],
                ["name" => "market_flag", "type" => \Swoole\Table::TYPE_STRING, "size" => 5],
                ["name" => "is_main", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "odd_label", "type" => \Swoole\Table::TYPE_STRING, "size" => 10],
                ["name" => "odds", "type" => \Swoole\Table::TYPE_FLOAT],
            ],
        ],
        "eventMarketsIndex"         => [
            "size"   => 100000,
            "column" => [
                ["name" => "bet_identifier", "type" => \Swoole\Table::TYPE_STRING, "size" => 20]
            ],
        ],
        "eventMarketList"           => [
            "size"   => 20000,
            "column" => [
                ["name" => "marketIDs", "type" => \Swoole\Table::TYPE_STRING, "size" => 3000]
            ],
        ],
        "masterEventMarkets"        => [
            "size"   => 100000,
            "column" => [
                ["name" => "is_main", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "master_event_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "master_event_market_unique_id", "type" => \Swoole\Table::TYPE_STRING, "size" => 32]
            ],
        ],
        "masterEventMarketsIndex"   => [
            "size"   => 100000,
            "column" => [
                ["name" => "master_event_market_id", "type" => \Swoole\Table::TYPE_INT]
            ],
        ],
        "masterEventMarketList"     => [
            "size"   => 20000,
            "column" => [
                ["name" => "memUIDs", "type" => \Swoole\Table::TYPE_STRING, "size" => 3000]
            ],
        ],
        "sportsOddTypes"            => [
            "size"   => 50,
            "column" => [
                ["name" => "value", "type" => \Swoole\Table::TYPE_INT]
            ],
        ],
        "activeOrders"              => [
            "size"   => 1000,
            "column" => [
                ["name" => "createdAt", "type" => \Swoole\Table::TYPE_STRING, "size" => 50],
                ["name" => "betId", "type" => \Swoole\Table::TYPE_STRING, "size" => 30],
                ["name" => "orderExpiry", "type" => \Swoole\Table::TYPE_STRING, "size" => 3],
                ["name" => "username", "type" => \Swoole\Table::TYPE_STRING, "size" => 20],
                ["name" => "userCurrencyId", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "status", "type" => \Swoole\Table::TYPE_STRING, "size" => 20],
            ],
        ],
        "statsCountEventsPerSecond" => [
            "size"   => 400,
            "column" => [
                ["name" => "total", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "processed", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "error", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveSport", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveProvider", "type" => \Swoole\Table::TYPE_FLOAT],
            ]
        ],
        "statsTimeEventsPerSecond"  => [
            "size"   => 400,
            "column" => [
                ["name" => "total", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "processed", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "error", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveSport", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveProvider", "type" => \Swoole\Table::TYPE_FLOAT],
            ]
        ],
        "statsCountOddsPerSecond"   => [
            "size"   => 400,
            "column" => [
                ["name" => "total", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "processed", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "error", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveSport", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveProvider", "type" => \Swoole\Table::TYPE_FLOAT],
            ]
        ],
        "statsTimeOddsPerSecond"    => [
            "size"   => 400,
            "column" => [
                ["name" => "total", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "processed", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "error", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveSport", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveProvider", "type" => \Swoole\Table::TYPE_FLOAT],
            ]
        ],
        "statsCountOddsPerSecond"   => [
            "size"   => 400,
            "column" => [
                ["name" => "total", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "processed", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "error", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveSport", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveProvider", "type" => \Swoole\Table::TYPE_FLOAT],
            ]
        ],
        "statsTimeOddsPerSecond"    => [
            "size"   => 400,
            "column" => [
                ["name" => "total", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "processed", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "error", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveSport", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "inactiveProvider", "type" => \Swoole\Table::TYPE_FLOAT],
            ]
        ],
        "requestTimers"             => [
            "size"   => 5,
            "column" => [
                ["name" => "value", "type" => \Swoole\Table::TYPE_INT],
            ]
        ],
        "requestIntervals"          => [
            "size"   => 4,
            "column" => [
                ["name" => "value", "type" => \Swoole\Table::TYPE_INT],
            ]
        ],
        "providerAccounts"          => [
            "size"   => 100,
            "column" => [
                ["name" => "provider_id", "type" => \Swoole\Table::TYPE_INT],
                ["name" => "username", "type" => \Swoole\Table::TYPE_STRING, "size" => 32],
                ["name" => "punter_percentage", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "credits", "type" => \Swoole\Table::TYPE_FLOAT],
                ["name" => "alias", "type" => \Swoole\Table::TYPE_STRING, "size" => 16],
                ['name' => 'type', 'type' => \Swoole\Table::TYPE_STRING, "size" => 50],
            ]
        ],
        'maintenance'               => [
            "size"   => 64,
            "column" => [
                ["name" => "under_maintenance", "type" => \Swoole\Table::TYPE_STRING, 'size' => 5],
            ]
        ],
        'walletClients'               => [
            "size"   => 64,
            "column" => [
                ["name" => "token", "type" => \Swoole\Table::TYPE_STRING, 'size' => 32],
            ]
        ],
    ],
    'logger' => [
        'app' => [
            'name' => 'app.log',
            'level' => 'debug'
        ],
        'odds-events-reactor' => [
            'name' => 'odds-event-reactor.log',
            'level' => 'debug'
        ],
        'odds' => [
            'name' => 'odds-process.log',
            'level' => 'debug'
        ],
        'event' => [
            'name' => 'event-process.log',
            'level' => 'debug'
        ]
    ]
];