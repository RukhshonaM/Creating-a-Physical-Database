-- =============================================================================
-- Physical Database: Mountaineering Club
-- Author  : Rukhshona Mirzarahmatova
-- Purpose : Physical implementation of the 3NF logical model for a
--           Mountaineering Club that organises climbing expeditions,
--           tracks participants, equipment, guides, routes, incidents
--           and weather observations.
-- DBMS    : PostgreSQL 15+
-- =============================================================================


-- =============================================================================
-- 1. DATABASE & SCHEMA SETUP
-- =============================================================================

-- Create a dedicated database (run as a superuser outside a transaction block).
-- Uncomment the two lines below and run them separately if needed:
-- CREATE DATABASE mountaineering_club
--     ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';

-- Connect to the database before running the rest:
-- \c mountaineering_club

-- Create a domain-related schema that groups all club objects together.
CREATE SCHEMA IF NOT EXISTS club;

-- Make all subsequent unqualified object names resolve to this schema.
SET search_path TO club, public;


-- =============================================================================
-- 2. TABLE DEFINITIONS
--    Order matters: referenced tables are created before referencing ones.
--    Each table uses BIGSERIAL surrogate PKs (no hardcoded IDs).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 2.1  countries
--      Lookup table for country names and ISO-3166-1 alpha-3 codes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS countries (
    country_id   BIGSERIAL    PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE,          -- UNIQUE constraint
    iso_code     CHAR(3)      NOT NULL UNIQUE            -- ISO 3-letter code, UNIQUE
);

-- ---------------------------------------------------------------------------
-- 2.2  areas
--      Geographic sub-regions within a country (e.g. "Himalayas", "Alps").
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS areas (
    area_id    BIGSERIAL    PRIMARY KEY,
    country_id BIGINT       NOT NULL
                            REFERENCES countries(country_id) ON DELETE RESTRICT,
    area_name  VARCHAR(150) NOT NULL,
    UNIQUE (country_id, area_name)                      -- no duplicate area per country
);

-- ---------------------------------------------------------------------------
-- 2.3  mountains
--      Core reference data for each mountain peak.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mountains (
    mountain_id BIGSERIAL      PRIMARY KEY,
    name        VARCHAR(150)   NOT NULL UNIQUE,
    height_m    INT            NOT NULL
                               CHECK (height_m > 0),   -- height must be positive (non-negative measured value)
    country_id  BIGINT         NOT NULL
                               REFERENCES countries(country_id) ON DELETE RESTRICT,
    area_id     BIGINT
                               REFERENCES areas(area_id) ON DELETE SET NULL,
    latitude    DECIMAL(9, 6)  NOT NULL,
    longitude   DECIMAL(9, 6)  NOT NULL,
    description TEXT
);

-- ---------------------------------------------------------------------------
-- 2.4  routes
--      Climbing routes on a specific mountain.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS routes (
    route_id             BIGSERIAL    PRIMARY KEY,
    mountain_id          BIGINT       NOT NULL
                                      REFERENCES mountains(mountain_id) ON DELETE RESTRICT,
    route_name           VARCHAR(200) NOT NULL,
    difficulty           VARCHAR(50)  NOT NULL
                                      CHECK (difficulty IN (
                                          'Beginner', 'Intermediate', 'Advanced', 'Expert'
                                      )),              -- CHECK: only allowed difficulty labels
    approx_distance_km   DECIMAL(7,2) NOT NULL
                                      CHECK (approx_distance_km >= 0),  -- non-negative measured value
    typical_duration_days INT         NOT NULL
                                      CHECK (typical_duration_days > 0),
    UNIQUE (mountain_id, route_name)
);

-- ---------------------------------------------------------------------------
-- 2.5  addresses
--      Reusable address records referenced by climbers and guides.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS addresses (
    address_id  BIGSERIAL    PRIMARY KEY,
    country_id  BIGINT       NOT NULL
                             REFERENCES countries(country_id) ON DELETE RESTRICT,
    area        VARCHAR(150),
    city        VARCHAR(100) NOT NULL,
    street      VARCHAR(255) NOT NULL,
    postal_code VARCHAR(20)  NOT NULL
);

-- ---------------------------------------------------------------------------
-- 2.6  climbers
--      Club members or guest climbers who participate in expeditions.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS climbers (
    climber_id              BIGSERIAL    PRIMARY KEY,
    first_name              VARCHAR(80)  NOT NULL,
    last_name               VARCHAR(80)  NOT NULL,
    birth_date              DATE         NOT NULL
                                         CHECK (birth_date > DATE '2000-01-01'),  -- date > Jan 1 2000 constraint
    gender                  VARCHAR(10)  NOT NULL
                                         CHECK (gender IN ('Male', 'Female', 'Other')), -- CHECK: specific allowed values (like gender)
    phone                   VARCHAR(30),
    email                   VARCHAR(150) UNIQUE,        -- UNIQUE constraint
    address_id              BIGINT       REFERENCES addresses(address_id) ON DELETE SET NULL,
    emergency_contact_name  VARCHAR(160),
    emergency_contact_phone VARCHAR(30),
    medical_notes           TEXT
);

-- ---------------------------------------------------------------------------
-- 2.7  guides
--      Professional expedition leaders with certifications.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS guides (
    guide_id       BIGSERIAL    PRIMARY KEY,
    first_name     VARCHAR(80)  NOT NULL,
    last_name      VARCHAR(80)  NOT NULL,
    phone          VARCHAR(30),
    email          VARCHAR(150) UNIQUE,                 -- UNIQUE constraint
    certifications TEXT,
    address_id     BIGINT       REFERENCES addresses(address_id) ON DELETE SET NULL
);

-- ---------------------------------------------------------------------------
-- 2.8  equipment
--      Club-owned inventory items (ropes, tents, crampons, etc.).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS equipment (
    equipment_id   BIGSERIAL    PRIMARY KEY,
    item_name      VARCHAR(150) NOT NULL,
    item_type      VARCHAR(80)  NOT NULL,
    quantity_total INT          NOT NULL DEFAULT 1
                                CHECK (quantity_total >= 0),  -- non-negative measured value
    notes          TEXT
);

-- ---------------------------------------------------------------------------
-- 2.9  climbs
--      Each row represents one expedition event.
--      planned_difficulty is a GENERATED column derived from route difficulty
--      (stored for quick reference; kept as a plain copy here because
--       cross-table GENERATED columns are not supported in PostgreSQL —
--       the column is populated via INSERT and validated by CHECK).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS climbs (
    climb_id           BIGSERIAL    PRIMARY KEY,
    mountain_id        BIGINT       NOT NULL
                                    REFERENCES mountains(mountain_id) ON DELETE RESTRICT,
    route_id           BIGINT
                                    REFERENCES routes(route_id) ON DELETE SET NULL,
    name               VARCHAR(200) NOT NULL,
    start_date         DATE         NOT NULL
                                    CHECK (start_date > DATE '2000-01-01'),  -- date > Jan 1 2000
    end_date           DATE         NOT NULL,
    planned_difficulty VARCHAR(50)  NOT NULL
                                    CHECK (planned_difficulty IN (
                                        'Beginner', 'Intermediate', 'Advanced', 'Expert'
                                    )),
    summary            TEXT,
    created_by         BIGINT       REFERENCES climbers(climber_id) ON DELETE SET NULL,
    -- Derived / computed column: duration in days (GENERATED ALWAYS AS)
    duration_days      INT          GENERATED ALWAYS AS
                                    (end_date - start_date) STORED,
    CONSTRAINT chk_dates CHECK (end_date >= start_date)
);

-- ---------------------------------------------------------------------------
-- 2.10  climb_participants  (junction: climbs M2M climbers)
--        Records which climber joined which climb and in what role.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS climb_participants (
    climb_id    BIGINT       NOT NULL
                             REFERENCES climbs(climb_id)   ON DELETE CASCADE,
    climber_id  BIGINT       NOT NULL
                             REFERENCES climbers(climber_id) ON DELETE CASCADE,
    role        VARCHAR(80)  NOT NULL DEFAULT 'Participant',
    joined_on   DATE
                             CHECK (joined_on IS NULL OR joined_on > DATE '2000-01-01'), -- date > Jan 1 2000
    notes       TEXT,
    PRIMARY KEY (climb_id, climber_id)                    -- composite PK (M2M junction)
);

-- ---------------------------------------------------------------------------
-- 2.11  climb_guides  (junction: climbs M2M guides)
--        Links one or more guides to each climb.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS climb_guides (
    climb_id  BIGINT      NOT NULL
                          REFERENCES climbs(climb_id)  ON DELETE CASCADE,
    guide_id  BIGINT      NOT NULL
                          REFERENCES guides(guide_id)  ON DELETE CASCADE,
    role      VARCHAR(80) NOT NULL DEFAULT 'Lead Guide',
    notes     TEXT,
    PRIMARY KEY (climb_id, guide_id)                      -- composite PK (M2M junction)
);

-- ---------------------------------------------------------------------------
-- 2.12  climb_equipment  (junction: climbs M2M equipment)
--        Tracks which equipment items were used on each climb.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS climb_equipment (
    climb_id          BIGINT NOT NULL
                             REFERENCES climbs(climb_id)     ON DELETE CASCADE,
    equipment_id      BIGINT NOT NULL
                             REFERENCES equipment(equipment_id) ON DELETE RESTRICT,
    quantity_used     INT    NOT NULL DEFAULT 1
                             CHECK (quantity_used >= 0),     -- non-negative measured value
    condition_on_return VARCHAR(80)
                             CHECK (condition_on_return IS NULL OR condition_on_return IN (
                                 'Excellent', 'Good', 'Fair', 'Damaged', 'Lost'
                             )),
    notes             TEXT,
    PRIMARY KEY (climb_id, equipment_id)                   -- composite PK (M2M junction)
);

-- ---------------------------------------------------------------------------
-- 2.13  weather_reports
--        Time-stamped weather observations linked to a specific climb.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_reports (
    weather_id    BIGSERIAL    PRIMARY KEY,
    climb_id      BIGINT       NOT NULL
                               REFERENCES climbs(climb_id) ON DELETE CASCADE,
    observed_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    temperature_c DECIMAL(5,2) NOT NULL,                  -- can be negative (frost)
    wind_kph      DECIMAL(6,2) NOT NULL
                               CHECK (wind_kph >= 0),     -- non-negative measured value
    conditions    VARCHAR(100) NOT NULL
                               CHECK (conditions IN (
                                   'Clear', 'Partly Cloudy', 'Overcast',
                                   'Light Snow', 'Heavy Snow', 'Blizzard',
                                   'Rain', 'Fog', 'Stormy'
                               )),
    notes         TEXT
);

-- ---------------------------------------------------------------------------
-- 2.14  incidents
--        Safety or medical events that occurred during a climb.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS incidents (
    incident_id        BIGSERIAL    PRIMARY KEY,
    climb_id           BIGINT       NOT NULL
                                    REFERENCES climbs(climb_id) ON DELETE CASCADE,
    reported_by_climber_id BIGINT   REFERENCES climbers(climber_id) ON DELETE SET NULL,
    incident_date      DATE         NOT NULL
                                    CHECK (incident_date > DATE '2000-01-01'), -- date > Jan 1 2000
    incident_time      TIME,
    severity           VARCHAR(20)  NOT NULL
                                    CHECK (severity IN ('Minor', 'Moderate', 'Serious', 'Critical')),
    description        TEXT         NOT NULL,
    action_taken       TEXT
);


-- =============================================================================
-- 3. SAMPLE DATA INSERTION
--    Uses DO $$ ... $$ anonymous blocks with INSERT ... ON CONFLICT DO NOTHING
--    so the script is idempotent (safe to run multiple times without
--    inserting duplicate rows).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 3.1  countries
-- ---------------------------------------------------------------------------
INSERT INTO countries (country_name, iso_code) VALUES
    ('Nepal',          'NPL'),
    ('Pakistan',       'PAK'),
    ('China',          'CHN'),
    ('India',          'IND'),
    ('Switzerland',    'CHE'),
    ('France',         'FRA'),
    ('United States',  'USA'),
    ('Kazakhstan',     'KAZ')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.2  areas
-- ---------------------------------------------------------------------------
INSERT INTO areas (country_id, area_name)
SELECT c.country_id, a.area_name
FROM (VALUES
    ('Nepal',       'Khumbu Region'),
    ('Nepal',       'Annapurna Region'),
    ('Pakistan',    'Karakoram Range'),
    ('China',       'Tibetan Plateau'),
    ('Switzerland', 'Bernese Alps'),
    ('France',      'Mont Blanc Massif'),
    ('United States','Sierra Nevada'),
    ('Kazakhstan',  'Tian Shan Range')
) AS a(country_name, area_name)
JOIN countries c ON c.country_name = a.country_name
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.3  mountains
-- ---------------------------------------------------------------------------
INSERT INTO mountains (name, height_m, country_id, area_id, latitude, longitude, description)
SELECT
    m.name, m.height_m,
    c.country_id,
    ar.area_id,
    m.latitude, m.longitude, m.description
FROM (VALUES
    ('Mount Everest',  8849, 'Nepal',        'Khumbu Region',
     27.988056::DECIMAL(9,6), 86.925278::DECIMAL(9,6),
     'Highest peak on Earth, located in the Mahalangur Himal sub-range.'),
    ('K2',             8611, 'Pakistan',     'Karakoram Range',
     35.880833::DECIMAL(9,6), 76.515278::DECIMAL(9,6),
     'Second highest peak; known as the Savage Mountain due to its difficulty.'),
    ('Annapurna I',    8091, 'Nepal',        'Annapurna Region',
     28.596111::DECIMAL(9,6), 83.820278::DECIMAL(9,6),
     'Tenth highest mountain; historically high fatality ratio.'),
    ('Cho Oyu',        8188, 'China',        'Tibetan Plateau',
     28.094167::DECIMAL(9,6), 86.660556::DECIMAL(9,6),
     'Sixth highest mountain; considered the easiest 8000m peak.'),
    ('Matterhorn',     4478, 'Switzerland',  'Bernese Alps',
     45.976389::DECIMAL(9,6),  7.658611::DECIMAL(9,6),
     'Iconic pyramidal peak on the Swiss-Italian border.'),
    ('Mont Blanc',     4808, 'France',       'Mont Blanc Massif',
     45.832778::DECIMAL(9,6),  6.865000::DECIMAL(9,6),
     'Highest summit in the Alps and Western Europe.'),
    ('Khan Tengri',    6995, 'Kazakhstan',   'Tian Shan Range',
     42.214722::DECIMAL(9,6), 80.174722::DECIMAL(9,6),
     'Marble pyramid peak on the Kazakhstan-China-Kyrgyzstan border.'),
    ('Mount Whitney',  4421, 'United States','Sierra Nevada',
     36.578583::DECIMAL(9,6),-118.291995::DECIMAL(9,6),
     'Highest summit in the contiguous United States.')
) AS m(name, height_m, country_name, area_name, latitude, longitude, description)
JOIN countries c  ON c.country_name = m.country_name
JOIN areas    ar  ON ar.area_name   = m.area_name AND ar.country_id = c.country_id
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.4  routes
-- ---------------------------------------------------------------------------
INSERT INTO routes (mountain_id, route_name, difficulty, approx_distance_km, typical_duration_days)
SELECT mt.mountain_id, r.route_name, r.difficulty, r.dist, r.dur
FROM (VALUES
    ('Mount Everest', 'South Col Route',           'Expert',       300.00, 55),
    ('Mount Everest', 'Northeast Ridge',           'Expert',       310.00, 60),
    ('K2',            'Abruzzi Spur',              'Expert',       120.00, 40),
    ('Annapurna I',   'North Face Direct',         'Expert',       180.00, 45),
    ('Cho Oyu',       'Northwest Ridge Normal',    'Advanced',     130.00, 30),
    ('Matterhorn',    'Hörnli Ridge',              'Advanced',      14.00,  2),
    ('Mont Blanc',    'Goûter Route',              'Intermediate',  18.00,  2),
    ('Khan Tengri',   'West Ridge',                'Advanced',      80.00, 18),
    ('Mount Whitney', 'Main Trail',                'Beginner',      22.50,  2),
    ('Mount Whitney', 'North Fork Lone Pine Creek','Intermediate',  35.00,  3)
) AS r(mtn_name, route_name, difficulty, dist, dur)
JOIN mountains mt ON mt.name = r.mtn_name
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.5  addresses
-- ---------------------------------------------------------------------------
INSERT INTO addresses (country_id, area, city, street, postal_code)
SELECT c.country_id, a.area, a.city, a.street, a.postal_code
FROM (VALUES
    ('Nepal',         'Bagmati Province', 'Kathmandu',  '14 Thamel Lane',           '44600'),
    ('Pakistan',      'Gilgit-Baltistan', 'Skardu',     '7 Hussainabad Road',       '16100'),
    ('Switzerland',   'Valais',           'Zermatt',    '3 Bahnhofstrasse',         '3920'),
    ('France',        'Auvergne-Rhône',   'Chamonix',   '22 Rue du Mont-Blanc',     '74400'),
    ('United States', 'California',       'Lone Pine',  '135 Whitney Portal Rd',    '93545'),
    ('Kazakhstan',    'Almaty Region',    'Almaty',     '8 Dostyk Avenue',          '050010'),
    ('India',         'Sikkim',           'Gangtok',    '56 M.G. Marg',             '737101'),
    ('Nepal',         'Gandaki Province', 'Pokhara',    '21 Lakeside Road',         '33700')
) AS a(country_name, area, city, street, postal_code)
JOIN countries c ON c.country_name = a.country_name
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.6  climbers
-- ---------------------------------------------------------------------------
INSERT INTO climbers (first_name, last_name, birth_date, gender, phone, email,
                      address_id, emergency_contact_name, emergency_contact_phone, medical_notes)
SELECT
    cl.first_name, cl.last_name, cl.birth_date::DATE, cl.gender,
    cl.phone, cl.email, ad.address_id,
    cl.emerg_name, cl.emerg_phone, cl.medical_notes
FROM (VALUES
    ('Asel',      'Nurbekova', '2001-03-15', 'Female', '+77012345678', 'asel.nurbekova@mail.kz',
     'Almaty',    'Daulet Nurbekov',  '+77019876543', NULL),
    ('Timur',     'Isakov',    '2000-07-22', 'Male',   '+77087654321', 'timur.isakov@mail.kz',
     'Almaty',    'Zarina Isakova',   '+77081234567', 'Mild asthma – carries inhaler'),
    ('Priya',     'Sharma',    '2002-11-05', 'Female', '+919876543210','priya.sharma@gmail.com',
     'Gangtok',   'Rajan Sharma',     '+919876543211', NULL),
    ('Carlos',    'Mendez',    '2001-06-18', 'Male',   '+14155550199', 'cmendez@yahoo.com',
     'Lone Pine', 'Maria Mendez',     '+14155550200', 'Allergic to penicillin'),
    ('Hana',      'Müller',    '2003-02-28', 'Female', '+41791234567', 'hana.mueller@bluewin.ch',
     'Zermatt',   'Klaus Müller',     '+41791234568', NULL),
    ('Liang',     'Wei',       '2000-09-10', 'Male',   '+8613987654321','lwei@sina.cn',
     'Kathmandu', 'Fang Wei',         '+8613987654322', 'Previous frostbite – right toes'),
    ('Sophie',    'Lefebvre',  '2004-04-12', 'Female', '+33612345678', 'sophie.lefebvre@free.fr',
     'Chamonix',  'Henri Lefebvre',   '+33612345679', NULL),
    ('Arjun',     'Thapa',     '2001-12-01', 'Male',   '+977984123456','arjun.thapa@nepal.com',
     'Pokhara',   'Sita Thapa',       '+977984123457', 'High-altitude acclimatisation history noted'),
    ('Elena',     'Volkov',    '2002-08-19', 'Female', '+77051234567', 'elena.volkov@mail.ru',
     'Almaty',    'Igor Volkov',      '+77051234568', NULL),
    ('Raj',       'Patel',     '2000-03-30', 'Male',   '+447911123456','raj.patel@gmail.com',
     'Skardu',    'Anita Patel',      '+447911123457', NULL)
) AS cl(first_name, last_name, birth_date, gender, phone, email,
        addr_city, emerg_name, emerg_phone, medical_notes)
JOIN addresses ad ON ad.city = cl.addr_city
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.7  guides
-- ---------------------------------------------------------------------------
INSERT INTO guides (first_name, last_name, phone, email, certifications, address_id)
SELECT g.first_name, g.last_name, g.phone, g.email, g.certs, ad.address_id
FROM (VALUES
    ('Pasang',    'Sherpa',  '+977980012345', 'pasang.sherpa@guide.np',
     'IFMGA Certified Mountain Guide; Everest Summit x7',         'Kathmandu'),
    ('Nima',      'Dorje',   '+977980098765', 'nima.dorje@guide.np',
     'IFMGA Certified; Wilderness First Responder',                'Pokhara'),
    ('Isabelle',  'Renard',  '+33698765432',  'isabelle.renard@guides-chamonix.fr',
     'UIAGM/IFMGA Certified; Chamonix Guide Service',             'Chamonix'),
    ('Hans',      'Brunner', '+41794567890',  'hans.brunner@bergfuhrer.ch',
     'Swiss Mountain Guide Association Member; Matterhorn specialist','Zermatt')
) AS g(first_name, last_name, phone, email, certs, addr_city)
JOIN addresses ad ON ad.city = g.addr_city
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.8  equipment
-- ---------------------------------------------------------------------------
INSERT INTO equipment (item_name, item_type, quantity_total, notes)
VALUES
    ('Dynamic Climbing Rope 60m',   'Rope',        8,  'Kernmantle; replaced annually'),
    ('Harness Set',                  'Harness',    15,  'Petzl Corax; sizes S/M/L'),
    ('Ice Axe 70cm',                 'Ice Tool',   12,  'Black Diamond Raven; marked with club ID'),
    ('Crampons 12-point',            'Footwear',   12,  'Grivel G12; fit most boot sizes'),
    ('Four-Season Tent (2-person)',   'Shelter',     6,  'MSR Advance Pro; rated to -40°C'),
    ('High-Altitude Sleeping Bag',   'Sleeping',    8,  'Western Mountaineering Puma MF; -40°C rating'),
    ('Portable Oxygen Set',          'Medical',     4,  'Summit O₂ system; 2 cylinders per set'),
    ('First Aid Kit',                'Medical',    10,  'Wilderness-grade; includes AED components'),
    ('Satellite Communicator',       'Electronics', 5,  'Garmin inReach Mini 2'),
    ('Avalanche Beacon',             'Safety',     20,  'Mammut Barryvox S; updated firmware 2024')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.9  climbs
-- ---------------------------------------------------------------------------
INSERT INTO climbs (mountain_id, route_id, name, start_date, end_date, planned_difficulty, summary, created_by)
SELECT
    mt.mountain_id, ro.route_id,
    cl.name, cl.start_date::DATE, cl.end_date::DATE, cl.difficulty, cl.summary,
    cr.climber_id
FROM (VALUES
    ('Mount Everest', 'South Col Route',        'Spring Everest 2023',
     '2023-04-01', '2023-05-25', 'Expert',
     'Full summit attempt via South Col; all 8 members reached summit.',   'Arjun', 'Thapa'),
    ('K2',            'Abruzzi Spur',           'K2 Summer Expedition 2023',
     '2023-07-01', '2023-08-10', 'Expert',
     'Successful summit; one participant turned back at Camp III.',          'Liang',  'Wei'),
    ('Matterhorn',    'Hörnli Ridge',           'Matterhorn Classic 2023',
     '2023-07-15', '2023-07-16', 'Advanced',
     'One-day ascent and descent; excellent summer conditions.',             'Hana',   'Müller'),
    ('Mont Blanc',    'Goûter Route',           'Mont Blanc Summer 2023',
     '2023-08-05', '2023-08-06', 'Intermediate',
     'Two-day ascent via Goûter Hut; mild weather throughout.',              'Sophie', 'Lefebvre'),
    ('Cho Oyu',       'Northwest Ridge Normal', 'Cho Oyu Autumn 2022',
     '2022-09-20', '2022-10-20', 'Advanced',
     'Acclimatisation-focused expedition; 6 of 8 reached the summit.',      'Raj',    'Patel'),
    ('Mount Whitney', 'Main Trail',             'Whitney Day Hike 2024',
     '2024-06-10', '2024-06-11', 'Beginner',
     'Annual club introductory hike; all participants completed the trail.', 'Carlos', 'Mendez'),
    ('Khan Tengri',   'West Ridge',             'Khan Tengri Summer 2024',
     '2024-07-10', '2024-07-28', 'Advanced',
     'High-altitude training expedition; summit reached by lead team.',      'Asel',   'Nurbekova'),
    ('Annapurna I',   'North Face Direct',      'Annapurna Spring 2024',
     '2024-04-15', '2024-05-30', 'Expert',
     'Most technically demanding expedition to date; all returned safely.',  'Timur',  'Isakov')
) AS cl(mtn, route, name, start_date, end_date, difficulty, summary, cr_fname, cr_lname)
JOIN mountains mt ON mt.name    = cl.mtn
JOIN routes    ro ON ro.route_name = cl.route AND ro.mountain_id = mt.mountain_id
JOIN climbers  cr ON cr.first_name = cl.cr_fname AND cr.last_name = cl.cr_lname
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.10  climb_participants  (M2M: climbs × climbers)
-- ---------------------------------------------------------------------------
INSERT INTO climb_participants (climb_id, climber_id, role, joined_on)
SELECT c.climb_id, cl.climber_id, cp.role, cp.joined_on::DATE
FROM (VALUES
    ('Spring Everest 2023',         'Arjun',   'Thapa',     'Team Leader',  '2023-03-15'),
    ('Spring Everest 2023',         'Liang',   'Wei',       'Participant',  '2023-03-15'),
    ('Spring Everest 2023',         'Raj',     'Patel',     'Participant',  '2023-03-16'),
    ('K2 Summer Expedition 2023',   'Liang',   'Wei',       'Team Leader',  '2023-06-01'),
    ('K2 Summer Expedition 2023',   'Timur',   'Isakov',    'Participant',  '2023-06-01'),
    ('K2 Summer Expedition 2023',   'Priya',   'Sharma',    'Participant',  '2023-06-02'),
    ('Matterhorn Classic 2023',     'Hana',    'Müller',    'Team Leader',  '2023-07-10'),
    ('Matterhorn Classic 2023',     'Carlos',  'Mendez',    'Participant',  '2023-07-10'),
    ('Mont Blanc Summer 2023',      'Sophie',  'Lefebvre',  'Team Leader',  '2023-07-30'),
    ('Mont Blanc Summer 2023',      'Elena',   'Volkov',    'Participant',  '2023-07-30'),
    ('Cho Oyu Autumn 2022',         'Raj',     'Patel',     'Team Leader',  '2022-09-05'),
    ('Cho Oyu Autumn 2022',         'Asel',    'Nurbekova', 'Participant',  '2022-09-05'),
    ('Whitney Day Hike 2024',       'Carlos',  'Mendez',    'Team Leader',  '2024-06-01'),
    ('Whitney Day Hike 2024',       'Sophie',  'Lefebvre',  'Participant',  '2024-06-01'),
    ('Khan Tengri Summer 2024',     'Asel',    'Nurbekova', 'Team Leader',  '2024-06-20'),
    ('Khan Tengri Summer 2024',     'Elena',   'Volkov',    'Participant',  '2024-06-20'),
    ('Annapurna Spring 2024',       'Timur',   'Isakov',    'Team Leader',  '2024-03-20'),
    ('Annapurna Spring 2024',       'Arjun',   'Thapa',     'Participant',  '2024-03-21'),
    ('Annapurna Spring 2024',       'Priya',   'Sharma',    'Participant',  '2024-03-21'),
    ('Annapurna Spring 2024',       'Raj',     'Patel',     'Participant',  '2024-03-22')
) AS cp(climb_name, cl_fname, cl_lname, role, joined_on)
JOIN climbs   c  ON c.name       = cp.climb_name
JOIN climbers cl ON cl.first_name = cp.cl_fname AND cl.last_name = cp.cl_lname
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.11  climb_guides  (M2M: climbs × guides)
-- ---------------------------------------------------------------------------
INSERT INTO climb_guides (climb_id, guide_id, role)
SELECT c.climb_id, g.guide_id, cg.role
FROM (VALUES
    ('Spring Everest 2023',       'Pasang', 'Sherpa',  'Lead Guide'),
    ('Spring Everest 2023',       'Nima',   'Dorje',   'Assistant Guide'),
    ('K2 Summer Expedition 2023', 'Pasang', 'Sherpa',  'Lead Guide'),
    ('K2 Summer Expedition 2023', 'Nima',   'Dorje',   'Assistant Guide'),
    ('Matterhorn Classic 2023',   'Hans',   'Brunner', 'Lead Guide'),
    ('Mont Blanc Summer 2023',    'Isabelle','Renard',  'Lead Guide'),
    ('Cho Oyu Autumn 2022',       'Nima',   'Dorje',   'Lead Guide'),
    ('Khan Tengri Summer 2024',   'Pasang', 'Sherpa',  'Lead Guide'),
    ('Annapurna Spring 2024',     'Pasang', 'Sherpa',  'Lead Guide'),
    ('Annapurna Spring 2024',     'Nima',   'Dorje',   'Assistant Guide')
) AS cg(climb_name, g_fname, g_lname, role)
JOIN climbs c ON c.name         = cg.climb_name
JOIN guides g ON g.first_name   = cg.g_fname AND g.last_name = cg.g_lname
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.12  climb_equipment  (M2M: climbs × equipment)
-- ---------------------------------------------------------------------------
INSERT INTO climb_equipment (climb_id, equipment_id, quantity_used, condition_on_return, notes)
SELECT c.climb_id, e.equipment_id, ce.qty, ce.cond, ce.notes
FROM (VALUES
    ('Spring Everest 2023',       'Dynamic Climbing Rope 60m',  4, 'Good',      NULL),
    ('Spring Everest 2023',       'Portable Oxygen Set',        4, 'Good',      '2 cylinders used per set'),
    ('Spring Everest 2023',       'Avalanche Beacon',           8, 'Excellent', NULL),
    ('K2 Summer Expedition 2023', 'Ice Axe 70cm',               6, 'Fair',      'Two handles worn'),
    ('K2 Summer Expedition 2023', 'Crampons 12-point',          6, 'Good',      NULL),
    ('K2 Summer Expedition 2023', 'First Aid Kit',              2, 'Good',      'One kit partially used'),
    ('Matterhorn Classic 2023',   'Harness Set',                4, 'Excellent', NULL),
    ('Matterhorn Classic 2023',   'Dynamic Climbing Rope 60m',  2, 'Good',      NULL),
    ('Mont Blanc Summer 2023',    'High-Altitude Sleeping Bag', 4, 'Excellent', NULL),
    ('Mont Blanc Summer 2023',    'Satellite Communicator',     1, 'Excellent', NULL),
    ('Cho Oyu Autumn 2022',       'Four-Season Tent (2-person)',3, 'Good',      NULL),
    ('Cho Oyu Autumn 2022',       'Portable Oxygen Set',        2, 'Good',      NULL),
    ('Whitney Day Hike 2024',     'First Aid Kit',              2, 'Excellent', NULL),
    ('Khan Tengri Summer 2024',   'Ice Axe 70cm',               4, 'Good',      NULL),
    ('Khan Tengri Summer 2024',   'Avalanche Beacon',           6, 'Excellent', NULL),
    ('Annapurna Spring 2024',     'Dynamic Climbing Rope 60m',  6, 'Fair',      'Surface abrasion noted'),
    ('Annapurna Spring 2024',     'Portable Oxygen Set',        4, 'Good',      NULL),
    ('Annapurna Spring 2024',     'First Aid Kit',              3, 'Good',      'Two kits partially used')
) AS ce(climb_name, item_name, qty, cond, notes)
JOIN climbs    c ON c.name      = ce.climb_name
JOIN equipment e ON e.item_name = ce.item_name
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.13  weather_reports
-- ---------------------------------------------------------------------------
INSERT INTO weather_reports (climb_id, observed_at, temperature_c, wind_kph, conditions, notes)
SELECT c.climb_id, wr.obs::TIMESTAMP, wr.temp, wr.wind, wr.cond, wr.notes
FROM (VALUES
    ('Spring Everest 2023',       '2023-04-15 06:00', -28.0,  35.5, 'Clear',          'Summit window opening'),
    ('Spring Everest 2023',       '2023-05-20 08:00', -32.0,  55.0, 'Partly Cloudy',  'High-altitude jet stream active'),
    ('K2 Summer Expedition 2023', '2023-07-20 07:00', -15.0,  80.0, 'Blizzard',       'Retreat to Camp II'),
    ('K2 Summer Expedition 2023', '2023-08-02 05:00', -10.0,  20.0, 'Clear',          'Summit attempt window'),
    ('Matterhorn Classic 2023',   '2023-07-15 05:30',   2.0,  15.0, 'Clear',          'Perfect alpine conditions'),
    ('Mont Blanc Summer 2023',    '2023-08-05 09:00',   5.0,  10.0, 'Clear',          'Ideal summer day'),
    ('Mont Blanc Summer 2023',    '2023-08-06 06:00',  -3.0,  25.0, 'Partly Cloudy',  'Light wind on summit ridge'),
    ('Cho Oyu Autumn 2022',       '2022-10-05 07:00', -20.0,  40.0, 'Heavy Snow',     'Day-3 storm; pinned at Camp I'),
    ('Cho Oyu Autumn 2022',       '2022-10-12 06:00', -18.0,  15.0, 'Clear',          'Summit attempt successful'),
    ('Annapurna Spring 2024',     '2024-05-10 05:00', -25.0,  60.0, 'Stormy',         'Major storm; 48h tent-bound'),
    ('Annapurna Spring 2024',     '2024-05-18 04:30', -22.0,  25.0, 'Clear',          'Post-storm summit window'),
    ('Khan Tengri Summer 2024',   '2024-07-20 06:00', -12.0,  30.0, 'Overcast',       'Cloud cover but manageable')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3.14  incidents
-- ---------------------------------------------------------------------------
INSERT INTO incidents
    (climb_id, reported_by_climber_id, incident_date, incident_time,
     severity, description, action_taken)
SELECT
    c.climb_id, cl.climber_id,
    i.inc_date::DATE, i.inc_time::TIME,
    i.severity, i.description, i.action_taken
FROM (VALUES
    ('Spring Everest 2023',       'Liang', 'Wei',
     '2023-05-01', '14:30', 'Minor',
     'Mild frostbite on right index finger at Camp III.',
     'Rewarming treatment applied; climber continued after 24h rest.'),
    ('K2 Summer Expedition 2023', 'Timur', 'Isakov',
     '2023-07-20', '09:00', 'Moderate',
     'Acute Mountain Sickness (AMS) symptoms: headache and nausea at Camp III.',
     'Climber descended to Camp II with guide escort; recovered within 18 hours.'),
    ('Annapurna Spring 2024',     'Priya', 'Sharma',
     '2024-05-12', '11:15', 'Moderate',
     'Rope anchor failure during fixed-line ascent; climber arrested by backup device.',
     'Anchor re-rigged; incident reviewed by lead guide; no injury sustained.'),
    ('Annapurna Spring 2024',     'Arjun', 'Thapa',
     '2024-05-10', '22:00', 'Serious',
     'Tent destroyed by storm gusts; equipment scattered.',
     'Team consolidated into two remaining tents; emergency bivouac protocol activated.'),
    ('Cho Oyu Autumn 2022',       'Raj',   'Patel',
     '2022-10-05', '16:00', 'Minor',
     'Participant slipped on icy slope; sustained bruised knee.',
     'First aid administered; climber rested one day and resumed ascent.')
) AS i(climb_name, cl_fname, cl_lname, inc_date, inc_time, severity, description, action_taken)
JOIN climbs   c  ON c.name        = i.climb_name
JOIN climbers cl ON cl.first_name = i.cl_fname AND cl.last_name = i.cl_lname
ON CONFLICT DO NOTHING;


-- =============================================================================
-- 4. ALTER TABLE – ADD record_ts TO EVERY TABLE
--    Each table gets a record_ts column with DEFAULT CURRENT_DATE.
--    If rows already exist (idempotent re-run), the column is only added once.
--    After adding, we UPDATE existing rows so record_ts reflects CURRENT_DATE.
-- =============================================================================

-- Helper macro: the pattern repeats for every table.
-- Step A – add the column (only if it does not already exist)
-- Step B – backfill existing rows

ALTER TABLE countries
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE countries SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE areas
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE areas SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE mountains
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE mountains SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE routes
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE routes SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE addresses
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE addresses SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE climbers
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE climbers SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE guides
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE guides SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE equipment
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE equipment SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE climbs
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE climbs SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE climb_participants
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE climb_participants SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE climb_guides
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE climb_guides SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE climb_equipment
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE climb_equipment SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE weather_reports
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE weather_reports SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;

ALTER TABLE incidents
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
UPDATE incidents SET record_ts = CURRENT_DATE WHERE record_ts IS NULL;


-- =============================================================================
-- 5. VERIFICATION QUERIES
--    Run these after the script to confirm record_ts was set for all rows
--    and that row counts meet requirements (≥ 2 per table, ≥ 20 total).
-- =============================================================================

/*
-- Row counts per table
SELECT 'countries'         AS tbl, COUNT(*) FROM countries
UNION ALL SELECT 'areas',               COUNT(*) FROM areas
UNION ALL SELECT 'mountains',           COUNT(*) FROM mountains
UNION ALL SELECT 'routes',              COUNT(*) FROM routes
UNION ALL SELECT 'addresses',           COUNT(*) FROM addresses
UNION ALL SELECT 'climbers',            COUNT(*) FROM climbers
UNION ALL SELECT 'guides',              COUNT(*) FROM guides
UNION ALL SELECT 'equipment',           COUNT(*) FROM equipment
UNION ALL SELECT 'climbs',              COUNT(*) FROM climbs
UNION ALL SELECT 'climb_participants',  COUNT(*) FROM climb_participants
UNION ALL SELECT 'climb_guides',        COUNT(*) FROM climb_guides
UNION ALL SELECT 'climb_equipment',     COUNT(*) FROM climb_equipment
UNION ALL SELECT 'weather_reports',     COUNT(*) FROM weather_reports
UNION ALL SELECT 'incidents',           COUNT(*) FROM incidents
ORDER BY tbl;

-- Verify record_ts is populated (no NULLs expected given NOT NULL DEFAULT)
SELECT 'countries'        , COUNT(*) FILTER (WHERE record_ts IS NULL) AS null_count FROM countries
UNION ALL SELECT 'climbers'      , COUNT(*) FILTER (WHERE record_ts IS NULL) FROM climbers
UNION ALL SELECT 'climbs'        , COUNT(*) FILTER (WHERE record_ts IS NULL) FROM climbs
UNION ALL SELECT 'incidents'     , COUNT(*) FILTER (WHERE record_ts IS NULL) FROM incidents;
*/


-- =============================================================================
-- END OF SCRIPT
-- =============================================================================
