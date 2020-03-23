#!/usr/bin/php
<?php
/*
    COVtrack
    Copyright (C) 2020 by Arjen Lentz <arjen@lentz.com.au>
    Licensed under GPLv3

    Tracking data from https://github.com/CSSEGISandData/COVID-19
*/

define ('TIMESERIES_CONFIRMED'  , 'time_series_19-covid-Confirmed.csv');
define ('TIMESERIES_DEATHS'     , 'time_series_19-covid-Deaths.csv');
define ('TIMESERIES_RECOVERED'  , 'time_series_19-covid-Recovered.csv');

define ('DB_NAME', 'covtrack');



function read_timeseries_csv($fname)
{
    $data = array();

    $fp = @fopen($fname, 'r');
    if (!$fp)
        die("Can't open '$fname'\n");

    $header = fgetcsv($fp);
    foreach ($header as $key => $value) {
        if (is_numeric($value[0])) {
            // convert mm/dd/yy to standard ISO yyyy-mm-dd
            $d = strptime($value, '%m/%d/%y');
            $header[$key] = sprintf('%04u-%02u-%02u', $d['tm_year']+1900, $d['tm_mon']+1, $d['tm_mday']);
        }
    }
    //print_r($header);
    $data['header'] = $header;

    while (!feof($fp)) {
        $arr = fgetcsv($fp);
        if (is_null($arr) || count($arr) < 5)
            continue;
        //print_r($arr);
        $data[trim($arr[0] . ' ' . $arr[1])] = $arr;
    }

    fclose($fp);
    return ($data);
}



// -------------------------------------
// get cmdline options

$shortopts = 'f:h:u:p:';    // filepath, dbhost, user, pwd

$options = getopt($shortopts);
$filepath = isset($options['f']) ? $options['f'] : './';           // default filepath to current dir
$db_host = isset($options['h']) ? $options['h'] : 'localhost';     // default dbhost to localhost
if (!isset($options['u']))
    die("Missing -u (db user) option\n");
$db_user = $options['u'];
$db_pass = isset($options['p']) ? $options['p'] : NULL;            // default user to no pwd



// -------------------------------------
// Read latest timeseries CSVs.
$confirmed = read_timeseries_csv($filepath.TIMESERIES_CONFIRMED);
$deaths = read_timeseries_csv($filepath.TIMESERIES_DEATHS);
$recovered = read_timeseries_csv($filepath.TIMESERIES_RECOVERED);

// Grab header row, then remove it from dataset arrays
$header = $confirmed['header'];
unset($confirmed['header']);
unset($deaths['header']);
unset($recovered['header']);


// Because we're processing all these three datasets in parallel,
// we have to ensure they're same # cols and same # cols, as it should be!
// Not an ideal input format really, but if we check it should be ok.
// They don't have the same country on the same row, so we do that differently already.
$rows = count($confirmed);
if ($rows != count($deaths) || $rows != count($recovered))
    die("The timeseries CSV files have different number of rows\n");

$cols = count($header);
foreach ($confirmed as $key => $row) {
/*
    Don't need to catch these, we'll deal with them on the fly
    if (!in_array($key, $deaths))
        print("Location '$key' from confirmed not in deaths\n");
    if (!in_array($key, $recovered))
        print("Location '$key' from confirmed not in recovered\n");
*/
    if ($cols != count($confirmed[$key]) || $cols != count($deaths[$key]) || $cols != count($recovered[$key])) {
        die("Different number of columns on timeseries row '$key'\n");
    }
}



// -------------------------------------
// Open db
$db = new mysqli($db_host, $db_user, $db_pass, DB_NAME);
if ($db->connect_error) {
    die('Connect Error (' . $db->connect_errno . ') ' . $db->connect_error);
}


/*
    CREATE TABLE locations (
        id          INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        stateprov   VARCHAR(50) NOT NULL,
        country     VARCHAR(50) NOT NULL,
        lat         DECIMAL(7,4),
        lon         DECIMAL(7,4),
        UNIQUE KEY  (stateprov,country),
        INDEX       (country)
    ) ENGINE=InnoDB;
*/

$put_location_query = 'INSERT IGNORE INTO locations (stateprov,country,lat,lon) VALUES (?,?,?,?)';
$put_location_stmt = $db->prepare($put_location_query);

// step through countries (skipping header line)
foreach ($confirmed as $key => $row) {
    $location_stateprov = $confirmed[$key][0];
    $location_country = $confirmed[$key][1];
    $location_lat = $confirmed[$key][2];
    $location_lon = $confirmed[$key][3];

    $put_location_stmt->bind_param('ssss', $location_stateprov, $location_country, $location_lat, $location_lon);
    $put_location_stmt->execute();
}
$put_location_stmt->close();


// creating a lookup array for stateprov/country -> id
$get_location_query = 'SELECT id,stateprov,country FROM locations';
$get_location_stmt = $db->prepare($get_location_query);
$get_location_stmt->execute();
$get_location_stmt->bind_result($location_id, $location_stateprov, $location_country);
$location_lookup = array();
while ($get_location_stmt->fetch()) {
    $key = trim($location_stateprov . ' ' . $location_country);
    $location_lookup[$key] = $location_id;
}
$get_location_stmt->close();


die("STOP MARK\n");


/*
    CREATE TABLE item (
        id              INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        location_id     INT UNSIGNED NOT NULL,
        recdate         DATE,
        confirmed_total INT UNSIGNED NOT NULL,
        deaths_total    INT UNSIGNED NOT NULL,
        recovered_total INT UNSIGNED NOT NULL,
        confirmed_new   INT UNSIGNED NOT NULL,
        deaths_new      INT UNSIGNED NOT NULL,
        recovered_new   INT UNSIGNED NOT NULL,
        UNIQUE KEY      (location_id,recdate),
        INDEX           (recdate)
    ) ENGINE=InnoDB;
*/

// try and insert, or update the data within the key (location_id,recdate)
$put_item_query = 'INSERT INTO item (location_id,recdate,confirmed_total,deaths_total,recovered_total,confirmed_new,deaths_new,recovered_new)'
            . ' VALUES (?,?,?,?,?,?,?,?)'
            . ' ON DUPLICATE KEY UPDATE '
                    . 'confirmed_total=?, deaths_total=?, recovered_total=?,'
                    . 'confirmed_new=?, deaths_new=?, recovered_new=?';
$put_item_stmt = $db->prepare($put_item_query);

// step through countries (skipping header line)
for ($i = 1; $i < $rows; $i++) {
    $location_key = trim($confirmed[$i][0] . ' ' . $confirmed[$i][1]);
    if (!in_array($location_lookup, $location_lookup))
        die("Location key $location_key not find in location lookup array\n");
    $location_id = $location_lookup[$location_key];

    $last_confirmed_total = $last_deaths_total = $last_recovered_total = 0;
    // step through dates within this country
    for ($col = 4; $col < $cols; $col++) {
        // grab date of this column
        $recdate = $header[$i][$col];

        // the numbers from the timeseries CSVs
        $confirmed_total    = $confirmed[$i][$col];
        $deaths_total       = $deaths[$i][$col];
        $recovered_total    = $recovered[$i][$col];

        // calculate some extra data while we're here
        $confirmed_new  = $confirmed_total  - $last_confirmed_total;
        $deaths_new     = $deaths_total     - $last_deaths_total;
        $recovered_new  = $recovered_total  - $last_recovered_total;

        $put_item_stmt->bind_param('isssssssssssss', $location_id, $recdate,
                                    $confirmed_total,$deaths_total,$recovered_total,$confirmed_new,$deaths_new,$recovered_new,
                                    $confirmed_total,$deaths_total,$recovered_total,$confirmed_new,$deaths_new,$recovered_new
                                );
        $put_item_stmt->execute();
    }

    $put_item_stmt->close();

    $db->close();
}


// end of file