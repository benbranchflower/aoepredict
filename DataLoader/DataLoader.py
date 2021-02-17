"""
This file defines a class with functionality to handle working with the data
so that the modeling files can be neater. I tried to make it easier to use
than navigating the SQL database itself but you can use sql_execute and
get_table to do any kind of query you would like. 

Ben Branchflower, benbranchflower@gmail.com
"""

import json
import re

import numpy as np
import pandas as pd
import psycopg2


class DataLoader():
    
    def __init__(self, config_file, save_ref_tables=True):
        """
        
        config_file (str): The name of the json file with the user info
        save_ref_tables (bool): if True, store the full reference tables instead
                            of just their queries
        """
        with open(config_file) as f:
            self.config = json.load(f)

        self.connection = self.make_conn(self.config)
        self.cursor = self.connection.cursor()
        
        # add queries for reference tables
        self.maps_query = "SELECT * FROM maps;"
        self.civbonus_query = "SELECT * FROM civilization_bonuses;"
        self.color_query = "SELECT * FROM player_colors;"
        self.civ_query = "SELECT * FROM civilizations;"
        self.platforms_query = "SELECT * FROM platforms;"
        self.mapsize_query = "SELECT * FROM map_sizes;"
        self.eventmap_query = "SELECT * FROM event_maps;"
        self.datasets_query = "SELECT * FROM datasets;"
        self.gametype_query = "SELECT * FROM game_types;"
        self.tech_query = "SELECT * FROM technologies;"
        self.difficulties_query = "SELECT * FROM difficulties;"
        self.mapreveal_query = "SELECT * FROM map_reveal_choices;"
        self.speed_query = "SELECT * FROM speeds;"
        self.startres_query = "SELECT * FROM starting_resources;"
        self.startage_query = "SELECT * FROM starting_ages;"
        self.victcond_query = "SELECT * FROM victory_conditions;"
        self.terrain_query = "SELECT * FROM terrain;"
        self.version_query = "SELECT * FROM versions;"
        self.action_query = "SELECT * FROM actions;"
        self.resource_query = "SELECT * FROM resources;"
        self.formationtype_query = "SELECT * FROM formation_types;"
        self.objects_query = "SELECT * FROM objects;"
        self.tournament_query = "SELECT * FROM tournaments;"
        self.ladder_query = "SELECT * FROM ladders;"
        
        # add queries for meta data
        self.participants_query = "SELECT * FROM participants;"
        self.player_query = "SELECT * FROM players;"
        self.series_query = "SELECT * FROM series;"
        self.seriesmeta_query = "SELECT * FROM series_metadata;"
        self.team_query = "SELECT * FROM teams;"
        self.matches_query = "SELECT * FROM matches;"
        
        # add queries for timestamped data
        self.objinststate_query = "SELECT * FROM object_instance_states;"
        self.actionlog_query = "SELECT * FROM action_log;"
        self.chat_query = "SELECT * FROM chat;"
        self.research_query = "SELECT * FROM research;"
        self.market_query = "SELECT * FROM market;"
        self.timeseries_query = "SELECT * FROM timeseries;"
        self.transaction_query = "SELECT * FROM transactions;"
        self.formation_query = "SELECT * FROM formations;"
        self.tribute_query = "SELECT * FROM tribute;"
        
        # add queries for misc tables
        self.tablename_query = "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='public';"
        self.users_query = "SELECT * FROM users;"
        self.files_query = "SELECT * FROM files;"
        self.people_query = "SELECT * FROM people;"
        self.hc_query = "SELECT * FROM hc;"
        self.event_query = "SELECT * FROM events;"
        self.objinst_query = "SELECT * FROM object_instances;"
        self.rounds_query = "SELECT * FROM rounds;"        
        if save_ref_tables:
            self.maps = self.get_table(self.maps_query, index='id')
            self.civbonus = self.get_table(self.civbonus_query, index='id')
            self.color = self.get_table(self.color_query, index='id')
            self.civ = self.get_table(self.civ_query, index='id')
            self.mapsize = self.get_table(self.mapsize_query, index='id')
            self.eventmap = self.get_table(self.eventmap_query, index='id')
            self.datasets = self.get_table(self.datasets_query, index='id')
            self.gametype = self.get_table(self.gametype_query, index='id')
            self.tech = self.get_table(self.tech_query, index='id')
            self.difficulties = self.get_table(self.difficulties_query, index='id')
            self.mapreveal = self.get_table(self.mapreveal_query, index='id')
            self.speed = self.get_table(self.speed_query, index='id')
            self.startres = self.get_table(self.startres_query, index='id')
            self.startage = self.get_table(self.startage_query, index='id')
            self.victcond = self.get_table(self.victcond_query, index='id')
            self.terrain = self.get_table(self.terrain_query, index='id')
            self.version = self.get_table(self.version_query, index='id')
            self.action = self.get_table(self.action_query, index='id')
            self.resource = self.get_table(self.resource_query, index='id')
            self.formation = self.get_table(self.formationtype_query, index='id')
            self.objects = self.get_table(self.objects_query, index='id')
            self.tournaments = self.get_table(self.tournament_query, index='id')
            self.ladders = self.get_table(self.ladder_query, index='id')
            self.platforms = self.get_table(self.platforms_query, index='id')
        
        # come column selections so you don't have to write them all everytime
        self.match_columns = 'id,series_id,tournament_id,event_id,version,minor_version,dataset_id,dataset_version,platform_id,ladder_id,rated,winning_team_id,builtin_map_id,map_size_id,event_map_id,rms_custom,rms_seed,fixed_positions,played,platform_match_id,duration ,completed,postgame,type_id,difficulty_id,population_limit,map_reveal_choice_id,cheats,speed_id,mirror,diplomacy_type,team_size,starting_resources_id,starting_age_id,victory_condition_id,all_technologies,version_id,multiqueue,treaty_length,build,version_id,starting_palisades,starting_town_centers,starting_walls,state_reader_interval,state_reader_version,platform_metadata,water_percent,server'
        
        # some custom id tables
        self.version
        
    
    def get_table(self, query, limit=None, index=None, **kwargs):
        """ Uses a read_sql to make a query as a wrapper for read_sql"""
        if limit is not None:
            query = query[:-1] + " LIMIT {0}".format(limit)
        try:
            return pd.read_sql(query, con=self.connection, index_col=index,
                               **kwargs)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self.make_conn(self.config)
            return pd.read_sql(query, con=self.connection, index_col=index,
                               **kwargs)
        
        
    def get_matches(self, day=19, month=11, year=2019, diplo='1v1', platform='voobly',
                    version='userpatch 1.5', maps=(9), limit=None,
                    dataset=1, ladder=131, map_size=120, rated='TRUE', 
                    completed='TRUE', columns=None, to_id=True):
        """
        Collect the meta data for the matches that are to be analyzed
        and merge onto timestamped data for the relevant matches
        
        day (int): day of month to be grabbed
        month (int): month of year to be grabbed
        year (int): year to be grabbed
        diplo (str): Type of game 1v1, TG or FFA
        platform (str): Which platfoorm to grab; voobly, vooblycn, de, ...
        version (str): What patch the games are to be played on
        limit (int): The maximum number of rows collected with the query
        maps (tuple): The mapids tp be collected, default is voobly Arabia
        dataset (int): The dataset id
        ladder (int): The ladder the games were played on
        map_size (int): THe size of the map
        rated (str): Whether or not the game was rated 'TRUE' or 'FALSE'
        completed (str): whether or not the game was completed, 'TRUE' or 'FALSE'
        to_id (bool): whether or not to convert verson and platform to integers
        """
        if columns is None:
            columns = self.match_columns
        query = "SELECT {0} FROM matches".format(columns)
        # FIXME: Add option to select dates
        # query += " WHERE EXTRACT(DAY FROM played)={0} AND EXTRACT(MONTH FROM played)={1} AND EXTRACT(YEAR FROM played)={2}".format(day, month, year)
        query += " WHERE TRUE"
        if diplo is not None:
            query += " AND diplomacy_type='{0}'".format(diplo)
        if platform is not None:
            query += " AND platform_id='{0}'".format(platform)
        if version is not None:
            query += " AND version='{0}'".format(version)
        if maps is not None:
            query += " AND builtin_map_id IN ({0})".format(maps)
        if dataset is not None:
            query += " AND dataset_id={0}".format(dataset)
        if ladder is not None:
            query += " AND ladder_id={0}".format(ladder)
        if map_size is not None:
            query += " AND map_size_id={0}".format(map_size)
        if rated is not None:
            query += " AND rated={0}".format(rated)
        if completed is not None:
            query += " AND completed={0}".format(completed)
        if limit is not None:
            query += " LIMIT {0}".format(limit)

        out = self.get_table(query)
       
        if to_id:
            version_dict = {x:y for x,y in zip(self.version.name, self.version.index)}
            if 'version' in out.columns:
                out.version = out.version.replace(version_dict)   
        
        self.match_indices = out.loc[:,'id']
        
        return out
    
    
    def get_timeseries(self, match_ids=None, round_seconds=True):
        """
        Get the basic timeseries info for the matches

        Parameters
        ----------
        match_ids : TYPE
            DESCRIPTION.

        Returns
        -------
        ts : TYPE
            DESCRIPTION.

        """
        if match_ids is None:
            match_ids = self.match_indices
        match_ids = tuple(match_ids)
        query = self.timeseries_query[:-1] + " WHERE match_id IN {0}".format(match_ids)
        ts = self.get_table(query)
        ts.set_index(['match_id','timestamp','player_number'], inplace=True)
        ts = ts.unstack()
        # print(ts)
        ts.columns = ['_'.join([x,str(y)]) for x,y in ts.columns.to_flat_index()]
        ts['timestamp'] = ts.index.get_level_values('timestamp')
        
        #if round_seconds:
        #    ts['timestamp'] = ts['timestamp'].astype('datetime64[s]') / np.timedelta64(1,'s')
        return ts
    
    
    def get_players(self, match_ids=None):
        """
        do a simple table of the players table to get info on match participants
        at the time of the match
        """
        if match_ids is None:
            match_ids = self.match_indices
        match_ids = tuple(match_ids)
        query = self.player_query[:-1] + " WHERE match_id IN {0}".format(match_ids)
        players = self.get_table(query)
        
        return players
    
    def get_teams(self, match_ids=None):
        """
        do a simple table of the players table to get info on match winners
        """
        if match_ids is None:
            match_ids = self.match_indices
        match_ids = tuple(match_ids)
        query = self.team_query[:-1] + " WHERE match_id IN {0}".format(match_ids)
        teams = self.get_table(query)
        
        return teams
    
    
    ''' # this should be a more general function when I get into doing team games
        # for now I will do the simple 1v1 draw
    def get_player_table(self):
        players = pd.read_sql("SELECT * FROM players WHERE match_id in {0}".format(tuple(self.match_indices)),
                      con=self.connection, index_col=None)
        players.set_index(['match_id','number'], inplace=True)
        players = players.unstack()
        players.columns = [c + "_{}".format(p) for c, p in players.columns.values]
        return players
    '''
        
    def sql_execute(self, query, **kwargs):
        """ do general custom queries in the database """
        return self.cursor.execute(query, **kwargs)
        
        
    def make_conn(self, config):
        """ establishes a connection with the aocrecs SQL server """
        return psycopg2.connect(f"dbname={config['dbname']} user={config['user']} password={config['password']} host={config['host']}")
            
if __name__ == '__main__':
    dl = DataLoader('../config.json')
    matches = dl.get_matches() # get match meta info
    timeseries = dl.get_timeseries().drop('timestamp', axis=1) # get timestamped game state data
    players = dl.get_players()
    teams = dl.get_teams()
    
    # merge match meta data to timeseries data for panel
    panel = timeseries.reset_index().merge(matches, 'left', left_on='match_id', right_on='id')
    # get game time in seconds for consistent timeseries variable
    panel['timestamp'] = panel.timestamp.dt.seconds + panel.timestamp.dt.microseconds/1000000
    
    
    
    
    
        