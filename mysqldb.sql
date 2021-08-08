create table artists (
	id VARCHAR(255), 
    name VARCHAR(255), 
    followers INTEGER, 
    popularity INTEGER, 
    url VARCHAR(255), 
    image_url VARCHAR(255), 
    PRIMARY KEY(id)) ENGINE=InnoDB
    DEFAULT CHARSET = 'utf8';

create table artist_genres (
	artist_id VARCHAR(255),
    genre VARCHAR(255),
    UNIQUE KEY(artist_id, genre)) ENGINE = InnoDB
    DEFAULT CHARSET = 'utf8';

select * from artists;
