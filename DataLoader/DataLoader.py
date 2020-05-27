"""
This file defines a class with functionality to handle
working with the data so that the modeling files can be
neater

Ben Branchflower, benbranchflower@gmail.com
"""

import json
import re

import psycopg2
import pandas as pd


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
        self.diff_query = "SELECT * FROM difficulties;"
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
        
        # add queries for meta data
        self.participants_query = "SELECT * FROM participants;"
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
        self.player_query = "SELECT * FROM players;"
        self.people_query = "SELECT * FROM people;"
        self.hc_query = "SELECT * FROM hc;"
        self.event_query = "SELECT * FROM events;"
        self.objinst_query = "SELECT * FROM object_instances;"
        self.rounds_query = "SELECT * FROM rounds;"
        self.ladder_query = "SELECT * FROM ladders;"
        
        if save_ref_tables:
            self.maps = self.get_table(self.maps_query)
            self.civbonus = self.get_table(self.civbonus_query)
            self.color = self.get_table(self.color_query)
            self.civ = self.get_table(self.civ_query)
            self.mapsize = self.get_table(self.mapsize_query)
            self.eventmap = self.get_table(self.eventmap_query)
            self.datasets = self.get_table(self.datasets_query)
            self.gametype = self.get_table(self.gametype_query)
            self.tech = self.get_table(self.tech_query)
            self.diff = self.get_table(self.diff_query)
            self.mapreveal = self.get_table(self.mapreveal_query)
            self.speed = self.get_table(self.speed_query)
            self.startres = self.get_table(self.startres_query)
            self.startage = self.get_table(self.startage_query)
            self.victcond = self.get_table(self.victcond_query)
            self.terrain = self.get_table(self.terrain_query)
            self.version = self.get_table(self.version_query)
            self.action = self.get_table(self.action_query)
            self.resource = self.get_table(self.resource_query)
            self.formation = self.get_table(self.formationtype_query)
            self.objects = self.get_table(self.objects_query)
            self.tournaments = self.get_table(self.tournament_query)
        
        # come column selections so you don't have to write them all everytime
        self.match_columns = 'id,series_id,tournament_id,event_id,version,minor_version,dataset_id,dataset_version,platform_id,ladder_id,rated,winning_team_id,builtin_map_id,map_size_id,map_name,event_map_id,rms_custom,rms_seed,fixed_positions,played,platform_match_id,duration ,completed,postgame,type_id,difficulty_id,population_limit,map_reveal_choice_id,cheats,speed_id,mirror,diplomacy_type,team_size,starting_resources_id,starting_age_id,victory_condition_id,all_technologies,version_id,multiqueue,treaty_length,build,version_id,starting_palisades,starting_town_centers,starting_walls,state_reader_interval,state_reader_version,platform_metadata,water_percent,server'
        
    
    def get_table(self, query, limit=None, **kwargs):
        """ Uses a read_sql to make a query as a wrapper for read_sql"""
        if limit is not None:
            query = query[:-1] + " LIMIT {0}".format(limit)
        try:
            return pd.read_sql(query, con=self.connection, **kwargs)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self.make_conn(self.config)
            return pd.read_sql(query, con=self.connection, **kwargs)
        
    def get_matches(self, day, month, year, columns=None):
        """
        Collect the meta data for the matches that are to be analyzed
        and merge onto timestamped data for the relevant matches
        """
        if columns is None:
            columns = self.match_columns
        query = "SELECT {0} FROM matches".format(columns)
        query += " WHERE EXTRACT(DAY FROM played)={0} AND EXTRACT(MONTH FROM played)={1} AND EXTRACT(YEAR FROM played)={2}".format(day, month, year)
        return self.get_table(query)
        
        
    def sql_execute(self, query, **kwargs):
        """ do general custom queries in the database """
        return self.cursor.execute(query, **kwargs)
        
        
    def make_conn(self, config):
        """ establishes a connection with the aocrecs SQL server """
        return psycopg2.connect(f"dbname={config['dbname']} user={config['user']} password={config['password']} host={config['host']}")
            
    
