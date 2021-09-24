CREATE TABLE users
(
    userid integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (userid)
);

CREATE TABLE movies
(
    movieid integer NOT NULL,
    title text,
    CONSTRAINT movies_pkey PRIMARY KEY (movieid)
);

CREATE TABLE genres
(
    genreid integer NOT NULL,
    name text,
    CONSTRAINT genres_pkey PRIMARY KEY (genreid)
);

CREATE TABLE taginfo
(
    tagid integer NOT NULL,
    content text,
    CONSTRAINT taginfo_pkey PRIMARY KEY (tagid)
);

CREATE TABLE hasagenre
(
    movieid integer NOT NULL,
    genreid integer NOT NULL,
    CONSTRAINT hasagenre_pkey PRIMARY KEY (movieid, genreid),
    CONSTRAINT genreid_constraint FOREIGN KEY (genreid)
        REFERENCES genres (genreid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT movieid_constraint FOREIGN KEY (movieid)
        REFERENCES movies (movieid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE ratings
(
    userid integer NOT NULL,
    movieid integer NOT NULL,
    rating numeric NOT NULL,
    "timestamp" bigint NOT NULL,
    CONSTRAINT ratings_pkey PRIMARY KEY (userid, movieid, rating, "timestamp"),
    CONSTRAINT movieid_constraint FOREIGN KEY (movieid)
        REFERENCES movies (movieid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT userid_constraint FOREIGN KEY (userid)
        REFERENCES users (userid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT rating_constraint CHECK (rating <= 5::numeric) NOT VALID
);

CREATE TABLE tags
(
    userid integer NOT NULL,
    movieid integer NOT NULL,
    tagid integer NOT NULL,
    "timestamp" bigint NOT NULL,
    CONSTRAINT tags_pkey PRIMARY KEY (userid, movieid, tagid, "timestamp"),
    CONSTRAINT movieid_constraint FOREIGN KEY (movieid)
        REFERENCES movies (movieid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT tagid_constraint FOREIGN KEY (tagid)
        REFERENCES taginfo (tagid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT userid_constraint FOREIGN KEY (userid)
        REFERENCES users (userid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);