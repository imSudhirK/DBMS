DROP TABLE ball_by_ball;
DROP TABLE player_match;
DROP TABLE match;
DROP TABLE team;
DROP TABLE player;
DROP TABLE venue;

--Team id and name
CREATE TABLE   team (
    team_id INT ,
    team_name TEXT,
    Primary key(team_id)
);

--Player information
CREATE TABLE player (
    player_id INT,
    player_name TEXT,
    dob DATE,
    batting_hand TEXT,
    bowling_skill TEXT,
    country_name TEXT,
    Primary Key(player_id)
);

--Venue information
CREATE TABLE venue (
    venue_id INT,
    venue_name TEXT,
    city_name TEXT,
    country_name TEXT,
    Primary Key(venue_id)
);

--Match information
CREATE TABLE   match (
    match_id INT,
    season_year INT, 
    team1 INT,
    team2 INT,
    venue_Id INT,
    toss_winner INT,
    match_winner INT,
    toss_name TEXT CHECK(toss_name='field' or toss_name='bat'),
    win_type TEXT CHECK(win_type='wickets' or win_type='runs' or win_type IS NULL),
    man_of_match INT,
    win_margin INT,
    PRIMARY KEY(match_id),
    FOREIGN KEY(venue_id) references venue on delete set null,
    FOREIGN KEY(team1) references team on delete set null,
    FOREIGN KEY(team2) references team on delete set null,
    FOREIGN KEY(toss_winner) references team on delete set null,
    FOREIGN KEY(match_winner) references team on delete set null,
    FOREIGN KEY(man_of_match) references player on delete set null
   
);

--For each match contains all players along with their role and team
CREATE TABLE   player_match (
    playermatch_key bigINT,
    match_id INT,
    player_id INT,
    role_desc TEXT CHECK(role_desc='Player' or role_desc='Keeper' or role_desc='CaptainKeeper' or role_desc='Captain'),
    team_id INT,
    PRIMARY KEY(playermatch_key),
    FOREIGN KEY(match_id) references match on delete set null,
    FOREIGN KEY(player_id) references player on delete set null,
    FOREIGN KEY(team_id) references team on delete set null
     
);

--Information for each ball
CREATE TABLE   ball_by_ball (
    match_id INT,
    innings_no INT CHECK(innings_no=1 or innings_no=2),  
    over_id INT,
    ball_id INT,
    runs_scored INT CHECK(runs_scored between 0 and 6),
    extra_runs INT,
    out_type TEXT CHECK(out_type='caught' or out_type='caught and bowled' or out_type='bowled' or out_type='stumped' or out_type='retired hurt' or out_type='keeper catch' or out_type='lbw'or out_type='run out' or out_type='hit wicket' or out_type IS NULL),
    striker INT,
    non_striker INT,
    bowler INT,
    PRIMARY KEY(match_id, innings_no, over_id,ball_id),
    Foreign Key (match_id) references match on delete set null,
    Foreign Key(striker) references player on delete set null,
    Foreign Key(non_striker) references player on delete set null,
    Foreign Key(bowler) references player on delete set null
);
