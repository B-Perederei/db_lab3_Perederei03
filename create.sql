CREATE TABLE Game (
	game_id int NOT NULL,
	game_name varchar(200) NOT NULL,
	game_genre varchar(40) NOT NULL,
	game_publisher varchar(80) NOT NULL
);

CREATE TABLE Platform (
	plat_id varchar(6) NOT NULL,
	plat_name varchar(60) NOT NULL
);

CREATE TABLE Region (
	region_id varchar(6) NOT NULL,
	region_name varchar(30) NOT NULL
);

CREATE TABLE GamePlatform (
	game_id int NOT NULL,
	plat_id varchar(6) NOT NULL,
	release_year numeric(4, 0)
);

CREATE TABLE GamePlatformRegionSales (
	game_id int NOT NULL,
	plat_id varchar(6) NOT NULL,
	region_id varchar(6) NOT NULL,
	date_counted date NOT NULL,
	copies_sold numeric(5, 2) NOT NULL
);

ALTER TABLE Game ADD CONSTRAINT PK_Game PRIMARY KEY (game_id);
ALTER TABLE Platform ADD CONSTRAINT PK_Platform PRIMARY KEY (plat_id);
ALTER TABLE Region ADD CONSTRAINT PK_Region PRIMARY KEY (region_id);

ALTER TABLE GamePlatform ADD CONSTRAINT PK_GamePlatform PRIMARY KEY (game_id, plat_id);
ALTER TABLE GamePlatformRegionSales ADD CONSTRAINT PK_GamePlatformRegionSales PRIMARY KEY (game_id, plat_id, region_id, date_counted);

ALTER TABLE GamePlatform ADD CONSTRAINT FK_game_id FOREIGN KEY (game_id) REFERENCES Game (game_id);
ALTER TABLE GamePlatform ADD CONSTRAINT FK_plat_id FOREIGN KEY (plat_id) REFERENCES Platform (plat_id);

ALTER TABLE GamePlatformRegionSales ADD CONSTRAINT FK_game_id FOREIGN KEY (game_id) REFERENCES Game (game_id);
ALTER TABLE GamePlatformRegionSales ADD CONSTRAINT FK_plat_id FOREIGN KEY (plat_id) REFERENCES Platform (plat_id);
ALTER TABLE GamePlatformRegionSales ADD CONSTRAINT FK_region_id FOREIGN KEY (region_id) REFERENCES Region (region_id);
