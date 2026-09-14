-- The three file-backed dimensions, seeded once when the warehouse is first
-- created. `customers` is not here: it arrives from the API, which is the point
-- of that part of the capstone.
--
-- These rows are the same as data/hubs.csv, data/service_levels.csv and
-- data/couriers.csv, which is what your pipeline reads to reject unknown hubs,
-- services and couriers. The warehouse should arrive already knowing its own
-- reference data - a pipeline is not responsible for creating the things it
-- validates against.
--
-- Like every file in init/, this runs only when postgres creates its data
-- directory. Changed it? `docker compose down -v` and start again.

INSERT INTO hubs (hub_id, hub_name, city, country, region) VALUES
    (1, 'Amman Central', 'Amman', 'Jordan', 'Levant'),
    (2, 'Zarqa Sort', 'Zarqa', 'Jordan', 'Levant'),
    (3, 'Irbid North', 'Irbid', 'Jordan', 'Levant'),
    (4, 'Aqaba Port', 'Aqaba', 'Jordan', 'Levant'),
    (5, 'Beirut Gateway', 'Beirut', 'Lebanon', 'Levant'),
    (6, 'Dubai Logistics City', 'Dubai', 'UAE', 'Gulf'),
    (7, 'Sharjah Annex', 'Sharjah', 'UAE', 'Gulf'),
    (8, 'Riyadh East', 'Riyadh', 'Saudi Arabia', 'Gulf'),
    (9, 'Jeddah Seaport', 'Jeddah', 'Saudi Arabia', 'Gulf'),
    (10, 'Cairo Ring', 'Cairo', 'Egypt', 'North Africa');

INSERT INTO service_levels (service_code, service_name, promised_hours) VALUES
    ('SDD', 'Same Day', 8),
    ('EXP', 'Express', 24),
    ('STD', 'Standard', 72),
    ('ECO', 'Economy', 120);

INSERT INTO couriers (courier_id, courier_name, hub_id, vehicle_type) VALUES
    (101, 'Dana Darwish', 2, 'truck'),
    (102, 'Basel Aziz', 3, 'van'),
    (103, 'Lina Fares', 4, 'motorbike'),
    (104, 'Tariq Khoury', 5, 'truck'),
    (105, 'Hala Sabbagh', 6, 'van'),
    (106, 'Yousef Sultan', 7, 'motorbike'),
    (107, 'Maya Nassar', 8, 'truck'),
    (108, 'Rami Mansour', 9, 'van'),
    (109, 'Rana Barakat', 10, 'motorbike'),
    (110, 'Karim Haddad', 1, 'truck'),
    (111, 'Aya Darwish', 2, 'van'),
    (112, 'Hadi Aziz', 3, 'motorbike'),
    (113, 'Salma Fares', 4, 'truck'),
    (114, 'Nabil Khoury', 5, 'van'),
    (115, 'Nour Sabbagh', 6, 'motorbike'),
    (116, 'Ziad Sultan', 7, 'truck'),
    (117, 'Reem Nassar', 8, 'van'),
    (118, 'Sami Mansour', 9, 'motorbike'),
    (119, 'Farah Barakat', 10, 'truck'),
    (120, 'Omar Haddad', 1, 'van'),
    (121, 'Dana Darwish', 2, 'motorbike'),
    (122, 'Basel Aziz', 3, 'truck'),
    (123, 'Lina Fares', 4, 'van'),
    (124, 'Tariq Khoury', 5, 'motorbike'),
    (125, 'Hala Sabbagh', 6, 'truck'),
    (126, 'Yousef Sultan', 7, 'van'),
    (127, 'Maya Nassar', 8, 'motorbike'),
    (128, 'Rami Mansour', 9, 'truck'),
    (129, 'Rana Barakat', 10, 'van'),
    (130, 'Karim Haddad', 1, 'motorbike'),
    (131, 'Aya Darwish', 2, 'truck'),
    (132, 'Hadi Aziz', 3, 'van'),
    (133, 'Salma Fares', 4, 'motorbike'),
    (134, 'Nabil Khoury', 5, 'truck'),
    (135, 'Nour Sabbagh', 6, 'van'),
    (136, 'Ziad Sultan', 7, 'motorbike'),
    (137, 'Reem Nassar', 8, 'truck'),
    (138, 'Sami Mansour', 9, 'van'),
    (139, 'Farah Barakat', 10, 'motorbike'),
    (140, 'Omar Haddad', 1, 'truck');
